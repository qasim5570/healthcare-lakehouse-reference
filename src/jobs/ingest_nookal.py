# Databricks notebook source
# ---------------------------------------------------------------------------
# Nookal -> landing volume.
#
# Rewritten against the actual Nookal PHP SDK (v0.1.14). Everything about the
# transport below was READ FROM THE SDK, not assumed:
#
#   Base URL   https://api.nookal.com                    API.php:70
#   Method     POST                                      Request.php CURLOPT_CUSTOMREQUEST
#   Body       application/x-www-form-urlencoded         Request.php CURLOPT_HTTPHEADER
#   Auth       api_key AS A FORM FIELD, not a header     Requests.php prepareConfig()
#   Records    data.results.<entity>                     types/Appointments.php:199
#   Paging     cursor via settings.nextPage              Response.php hasNextPage()
#   Errors     HTTP 200 can carry status:'failure'       Response.php constructor
#
# The REQUEST PARAMETERS below came from Nookal's HTTP documentation, NOT the
# SDK. The SDK cannot supply them: prepareConfig() forwards the caller's array
# untouched, so no parameter name appears anywhere in its code. Marked [DOCS]
# where the fields table for getAppointments was the source.
#
# Two things remain genuinely unknown, both about rate limiting, both only
# answerable by the vendor or by observation. Neither blocks development.
#
# This is the ONLY place plain Python runs. The bottleneck is a rate-limited
# HTTP endpoint, not compute, so there is nothing for Spark to do. Do not fan
# API calls across executors: you get throttled, partial extractions become
# hard to reason about, and token refresh stops being deterministic.
# ---------------------------------------------------------------------------

import json
import random
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from databricks.sdk.runtime import dbutils

from avanti.ops import audit_finish, audit_start, commit_watermark, read_watermark
from avanti.params import param
from avanti.transforms import landing_path

CATALOG = param("catalog")
SCHEMA = param("landing_schema")
OPS = param("ops_schema")
SOURCE = param("source")
ENTITY = param("entity")
MODE = param("mode", allowed={"sample", "api"})

# ---------------------------------------------------------------------------
# Per-entity configuration, all read from the SDK.
#
# NOTE the inconsistency: appointments carry `lastModified`, patients carry
# `DateModified`. Different casing, different word. Several entities appear to
# carry NO modification field at all, which means they cannot be extracted
# incrementally and must be full-refreshed.
#
# This is exactly why the field name is config rather than a hardcoded string.
# ---------------------------------------------------------------------------
ENTITY_CONFIG = {
    "appointments": {
        "path": "/production/v2/getAppointments",
        "results_key": "appointments",       # types/Appointments.php:199
        "modified_field": "lastModified",    # types/Appointments.php:25
    },
    "patients": {
        "path": "/production/v2/getPatients",
        "results_key": "patients",           # types/Patients.php:265
        "modified_field": "DateModified",    # types/Patients.php:70  <- different!
    },
    "practitioners": {
        "path": "/production/v2/getPractitioners",
        "results_key": "practitioners",
        "modified_field": None,              # no modification field -> full refresh
    },
    "locations": {
        "path": "/production/v2/getLocations",
        "results_key": "locations",
        "modified_field": None,
    },
    "invoices": {
        "path": "/production/v2/getInvoices",
        "results_key": "invoices",
        "modified_field": None,              # confirm against the docs
    },
    "appointment_types": {
        "path": "/production/v2/getAppointmentTypes",
        "results_key": "services",           # confirm: types/Service.php
        "modified_field": None,
    },
}

if ENTITY not in ENTITY_CONFIG:
    raise ValueError(
        f"entity '{ENTITY}' is not configured. Known: {sorted(ENTITY_CONFIG)}. "
        "Add it to ENTITY_CONFIG with its path, results_key and modified_field, "
        "all readable from the Nookal SDK."
    )

CFG = ENTITY_CONFIG[ENTITY]
BASE_URL = "https://api.nookal.com"
VERIFY_PATH = "/production/v2/verify"        # cheap endpoint to test credentials

# [DOCS] paging parameter. Default 1, must be > 0. The response carries
# settings.nextPage telling you the NEXT page number; you send it back under
# this name. The SDK showed the response half; only the docs give the request
# half.
PAGE_PARAM = "page"

# [DOCS] the modification filter EXISTS, so incremental extraction is
# server-side. Confirmed from the getAppointments optional-fields table.
#
# Set to False to fall back to client-side filtering: pull everything, discard
# what is older than the watermark. Correct but wasteful — every run transfers
# the full history. Useful for any entity whose endpoint lacks the filter.
INCREMENTAL_FILTER_SUPPORTED = True
MODIFIED_SINCE_PARAM = "last_modified"

# The docs state YYYY-MM-DD for date_from / date_to but give NO format rule for
# last_modified. Date-only would lose intra-day precision and re-pull a whole
# day each run — harmless, since Silver dedupes, but worth confirming.
# Verify by calling with a timestamp and checking details.totalItems drops.
MODIFIED_SINCE_FORMAT = "%Y-%m-%d %H:%M:%S"

# UNKNOWN — rate limits. Not in the SDK, not in the fields table. Throttle
# conservatively rather than discovering the limit by being blocked.
SECONDS_BETWEEN_CALLS = 0.5

# The business day, in the clinic's timezone, NOT the cluster's. Serverless runs
# in UTC, so date.today() at 05:00 Sydney returns the PREVIOUS day and records
# land in the wrong dt= partition.
BUSINESS_TZ = ZoneInfo("Australia/Sydney")
RUN_DATE = datetime.now(BUSINESS_TZ).date().isoformat()

# Stable across task retries, unlike uuid4(). resources/jobs.yml passes
# {{job.run_id}}, so a retried task reuses the batch id and overwrites its own
# files instead of writing a second set under a new name.
try:
    BATCH_ID = param("run_id")
except ValueError:
    BATCH_ID = f"manual-{uuid.uuid4()}"

random.seed(42)          # reruns produce identical sample data

PAGE_SIZE = 200          # [DOCS] page_length: default 100, MAXIMUM 200
MAX_PAGES = 500          # circuit breaker: a paging bug must not loop forever
RETRY_ATTEMPTS = 5

# COMMAND ----------


def generate_sample_page(cursor, since: datetime) -> tuple[list[dict], object]:
    """Stand-in for the Nookal API until credentials arrive.

    Returns (records, next_cursor) to match fetch_api_page's contract exactly,
    so the pagination loop is identical in both modes.

    Field names mirror the SDK: ID, patientID, appointmentDate, DNA, cancelled,
    lastModified. Deliberately messy values — five phone formats, mixed-case
    emails, missing fields, an unmapped 'wibble' status. Clean sample data would
    let through exactly the bugs real data catches.
    """
    page = 1 if cursor is None else int(cursor)
    if page > 3:
        return [], None

    statuses = ["completed", "Completed", "cancelled", "DNA", "no show",
                "booked", "rescheduled", "attended", "wibble"]
    phone_formats = ["0412 345 {n:03d}", "+61 412 345 {n:03d}", "0412345{n:03d}",
                     "(04) 1234 5{n:03d}", "61412345{n:03d}"]
    first = ["Sarah", "James", "Priya", "Wei", "Mohammed", "Emma", "Liam", "Aroha"]
    last = ["Mitchell", "Nguyen", "Patel", "Chen", "Okafor", "Wilson", "Brown"]

    out = []
    for i in range(PAGE_SIZE):
        n = (page - 1) * PAGE_SIZE + i
        start = datetime.now(timezone.utc) - timedelta(
            days=random.randint(0, 120), hours=random.randint(8, 17)
        )
        # Spread modification times rather than all being "now", so the Silver
        # dedupe window (which orders by _source_updated_ts) has something real
        # to resolve. With every record modified at the same instant, that path
        # is never exercised.
        modified = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 2880))
        cancelled = random.random() < 0.12

        out.append({
            "ID": 100000 + n,
            "patientID": 5000 + (n % 900),
            "practitionerID": 10 + (n % 12),
            "locationID": 1 + (n % 6),
            "appointmentDate": start.date().isoformat(),
            "appointmentStartTime": start.strftime("%H:%M"),
            "appointmentEndTime": (start + timedelta(minutes=30)).strftime("%H:%M"),
            "appointmentType": random.choice(["Initial", "Standard", "Extended"]),
            "appointmentTypeID": 1 + (n % 4),
            "arrived": random.choice(["0", "1"]),
            "invoiceGenerated": random.choice(["0", "1"]),
            "emailReminderSent": random.choice(["0", "1"]),
            "DNA": "1" if random.random() < 0.08 else "0",
            "cancelled": "1" if cancelled else "0",
            "cancellationDate": start.date().isoformat() if cancelled else None,
            "Notes": random.choice(
                [None, None, "patient called, sick", "car broke down",
                 "double booked by mistake", "work conflict"]
            ),
            "status_raw": random.choice(statuses),   # sample only; not a real Nookal field
            "patient_first_name": random.choice(first),
            "patient_last_name": random.choice(last),
            "patient_email": random.choice(
                [f"user{n % 900}@example.com", f"User{n % 900}@Example.COM", None]
            ),
            "patient_phone": random.choice(phone_formats).format(n=n % 1000),
            "patient_dob": (date(1950, 1, 1) + timedelta(days=(n * 37) % 20000)).isoformat(),
            "postcode": f"2{(n % 900):03d}",
            "lastModified": modified.strftime("%Y-%m-%d %H:%M:%S"),
            "dateCreated": (start - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),
        })

    # Deliberate duplicates with a LATER lastModified, so last-writer-wins in
    # Silver has something to resolve.
    for dup in out[:5]:
        later = dict(dup)
        later["lastModified"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        later["cancelled"] = "1"
        out.append(later)

    return out, (page + 1 if page < 3 else None)


def _nookal_post(path: str, payload: dict) -> dict:
    """One POST to Nookal, with retry and backoff. Returns the parsed envelope.

    Transport per the SDK: POST, form-encoded body, api_key as a body field.
    Note `data=` and NOT `params=` — params would put everything in the query
    string, the server would see no api_key, and the error would talk about
    authentication rather than about the mistake.
    """
    import requests

    api_key = dbutils.secrets.get(scope="avanti", key="nookal_api_key")
    body = {**payload, "api_key": api_key}          # Requests.php prepareConfig()

    for attempt in range(RETRY_ATTEMPTS):
        try:
            resp = requests.post(
                f"{BASE_URL}{path}",
                data=body,                          # FORM BODY, not query string
                timeout=60,
            )

            # 429 and 5xx are transient. Honour Retry-After when sent, else
            # exponential backoff WITH JITTER — without jitter, parallel
            # extractors all retry at the same instant and re-trigger the
            # throttle.
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = int(resp.headers.get("Retry-After", 2 ** attempt)) + random.uniform(0, 1)
                print(f"[api] HTTP {resp.status_code}, retry {attempt + 1}/{RETRY_ATTEMPTS} in {wait:.1f}s")
                time.sleep(wait)
                continue

            resp.raise_for_status()                 # catches other 4xx/5xx
            envelope = resp.json()

            # CRITICAL: Nookal returns HTTP 200 with status:'failure' for
            # application errors. raise_for_status() sees a 200 and is happy.
            # Without this check a failed call looks like an empty result, so
            # the extractor lands zero records and reports success.
            if envelope.get("status") == "failure":
                details = envelope.get("details", {})
                raise RuntimeError(
                    f"Nookal API failure: {details.get('errorMessage')} "
                    f"(code {details.get('errorCode')})"
                )

            return envelope

        except requests.RequestException as exc:
            if attempt == RETRY_ATTEMPTS - 1:
                raise
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f"[api] {type(exc).__name__}, retry {attempt + 1}/{RETRY_ATTEMPTS} in {wait:.1f}s")
            time.sleep(wait)

    raise RuntimeError(f"{path} failed after {RETRY_ATTEMPTS} attempts")


def fetch_api_page(cursor, since: datetime) -> tuple[list[dict], object]:
    """Fetch one page. Returns (records, next_cursor); next_cursor None = done.

    Cursor pagination per Response.php: the response carries settings.nextPage,
    null when there are no more pages.
    """
    # Other documented optional filters, not currently used:
    #   date_from / date_to    YYYY-MM-DD, filter on APPOINTMENT date
    #   appt_status            comma-separated: Cancelled, DNA, Completed, Uninvoiced
    #   location_id            scope to one clinic
    #   practitioner_id, patient_id, service_id, class_id
    #   time_from / time_to    hh:mm:ss
    payload = {"page_length": PAGE_SIZE}

    if cursor is not None:
        payload[PAGE_PARAM] = cursor            # [DOCS] "page"

    if INCREMENTAL_FILTER_SUPPORTED:            # [DOCS] "last_modified"
        payload[MODIFIED_SINCE_PARAM] = since.strftime(MODIFIED_SINCE_FORMAT)

    envelope = _nookal_post(CFG["path"], payload)

    records = (
        envelope.get("data", {})
        .get("results", {})
        .get(CFG["results_key"], [])            # types/<Entity>.php
    )

    # If the API cannot filter server-side, discard old records here. Correct
    # but wasteful — every run transfers the full history.
    if not INCREMENTAL_FILTER_SUPPORTED and CFG["modified_field"]:
        records = [r for r in records if _parse_modified(r) >= since]

    next_cursor = envelope.get("settings", {}).get("nextPage")   # Response.php
    time.sleep(SECONDS_BETWEEN_CALLS)                            # rate limit UNKNOWN
    return records, next_cursor


def _parse_modified(record: dict) -> datetime:
    """Read the entity's modification timestamp. Field name differs per entity."""
    field = CFG["modified_field"]
    if not field:
        return datetime.min.replace(tzinfo=timezone.utc)

    raw = record.get(field)
    if not raw:
        return datetime.min.replace(tzinfo=timezone.utc)

    parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# COMMAND ----------

fetch = generate_sample_page if MODE == "sample" else fetch_api_page

# Fail fast on bad credentials before doing any work. `verify` is the cheap
# endpoint the SDK exposes for exactly this.
if MODE == "api":
    _nookal_post(VERIFY_PATH, {})
    print("[api] credentials verified")

# WATERMARK — read BEFORE extracting. Returns the last successful high-water
# mark minus a 48h overlap, or a 2-year backfill on the first run.
# Entities with no modification field cannot be incremental, so they full-refresh.
if CFG["modified_field"]:
    since = read_watermark(CATALOG, OPS, SOURCE, ENTITY)
else:
    since = datetime.min.replace(tzinfo=timezone.utc)
    print(f"[watermark] {ENTITY} has no modification field — full refresh")

# AUDIT — written BEFORE any work. A row left as RUNNING with no finished_at
# means the run died mid-flight, which is the state you want visible.
audit_start(CATALOG, OPS, SOURCE, ENTITY, BATCH_ID)

target_dir = landing_path(CATALOG, SCHEMA, SOURCE, ENTITY, RUN_DATE)

total_records = 0
pages_written = 0
# The new watermark is the MAX modification timestamp actually SEEN, not now().
# now() would skip anything modified between the API responding and the commit.
max_seen = since

try:
    dbutils.fs.mkdirs(target_dir)

    cursor = None
    for page_num in range(MAX_PAGES):
        records, cursor = fetch(cursor, since)

        if records:
            # Idempotent filename keyed by batch_id (the job run id), so a
            # retried task overwrites its own files rather than writing a
            # second set under a new name.
            file_path = f"{target_dir}/{BATCH_ID}_{page_num:05d}.json"
            dbutils.fs.put(
                file_path,
                "\n".join(json.dumps(r) for r in records),   # newline-delimited JSON
                overwrite=True,
            )
            if CFG["modified_field"]:
                max_seen = max([max_seen] + [_parse_modified(r) for r in records])

            total_records += len(records)
            pages_written += 1

        # Cursor pagination: the RESPONSE tells us whether there is more.
        if cursor is None:
            break
    else:
        raise RuntimeError(
            f"hit MAX_PAGES={MAX_PAGES} with a non-null cursor — check paging logic"
        )

except Exception as exc:
    # AUDIT — record the failure, then re-raise so the task actually fails.
    # WATERMARK — deliberately NOT committed. The next run re-reads from the
    # old position, so nothing is skipped. Committing on partial success
    # creates a permanent gap that reports as a successful run.
    audit_finish(CATALOG, OPS, BATCH_ID, "FAILED", total_records, pages_written, str(exc))
    raise

# WATERMARK — commit only now, after every page succeeded.
if CFG["modified_field"] and total_records:
    commit_watermark(CATALOG, OPS, SOURCE, ENTITY, max_seen, BATCH_ID)

audit_finish(CATALOG, OPS, BATCH_ID, "SUCCEEDED", total_records, pages_written)

print(f"landed {total_records} records across {pages_written} pages")
print(f"batch_id:  {BATCH_ID}")
print(f"path:      {target_dir}")
if CFG["modified_field"]:
    print(f"watermark: {max_seen:%Y-%m-%d %H:%M:%S}")

dbutils.notebook.exit(json.dumps({
    "batch_id": BATCH_ID,
    "records": total_records,
    "pages": pages_written,
    "path": target_dir,
    "watermark": max_seen.isoformat() if CFG["modified_field"] else None,
}))
