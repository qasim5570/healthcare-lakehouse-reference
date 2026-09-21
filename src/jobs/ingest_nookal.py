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

from lakehouse.ops import audit_finish, audit_start, commit_watermark, read_watermark
from lakehouse.params import param
from lakehouse.transforms import landing_path

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

# ---------------------------------------------------------------------------
# SAMPLE VOLUME — mode=sample only. Ignored entirely when mode=api.
#
# Three pages over 120 days was enough to prove the pipeline and far too thin
# for a dashboard: utilisation sat near 2%, every patient fell in the lowest
# value band, and there was no prior year to compare against. These numbers
# produce roughly 4,000 appointments across 14 months, which gives the marts
# something with shape.
# ---------------------------------------------------------------------------
# 150 pages x 200 = ~30,000 appointments over 16 months. That works out at
# roughly 15 per clinic per day across six clinics, which is a plausible
# allied-health load and — critically — puts utilisation in the 70% range
# instead of the 9% you get when twelve practitioners share 250 appointments a
# month. A dashboard showing 9% utilisation looks broken, not informative.
SAMPLE_PAGES = 150            # 150 x 200 = ~30,000 appointments
SAMPLE_DAYS_BACK = 430        # ~14 months, so year-on-year comparison works
SAMPLE_DAYS_FORWARD = 30      # future bookings, which must show as 'booked'
SAMPLE_PATIENTS = 850         # ~34 visits each across 16 months, long-tailed

# COMMAND ----------


def generate_sample_page(cursor, since: datetime) -> tuple[list[dict], object]:
    """Stand-in for the Nookal API until credentials arrive.

    Returns (records, next_cursor) to match fetch_api_page's contract exactly,
    so the pagination loop is identical in both modes.

    Field names mirror the SDK: ID, patientID, appointmentDate, DNA, cancelled,
    lastModified. Deliberately messy values — five phone formats, mixed-case
    emails, missing fields. Clean sample data would let through exactly the
    bugs real data catches.

    Realism that matters for the marts, and did not exist in the first version:
      - appointments span 14 months, so year-on-year comparison is possible
      - FUTURE appointments are 'booked'; past ones resolve to completed, DNA
        or cancelled. Previously status was random regardless of date, which
        left half of all HISTORIC appointments sitting as 'booked'
      - duration varies and the end time agrees with it, so duration_min in
        Silver is derived from two fields that actually match
      - patient visit counts follow a long tail rather than a flat modulo, so
        lifetime-value banding separates instead of putting everyone in 'Low'
      - a January dip, because clinics genuinely are quiet over the holidays
    """
    page = 1 if cursor is None else int(cursor)
    if page > SAMPLE_PAGES:
        return [], None

    phone_formats = ["0412 345 {n:03d}", "+61 412 345 {n:03d}", "0412345{n:03d}",
                     "(04) 1234 5{n:03d}", "61412345{n:03d}"]
    first = ["Sarah", "James", "Priya", "Wei", "Mohammed", "Emma", "Liam", "Aroha",
             "Daniel", "Sophie", "Raj", "Chloe", "Hannah", "Tom", "Ana", "Yusuf"]
    last = ["Mitchell", "Nguyen", "Patel", "Chen", "Okafor", "Wilson", "Brown",
            "Taylor", "Singh", "Kaur", "Novak", "Ferrari", "Haddad", "Lee"]
    durations = [15, 30, 30, 30, 45, 45, 60]

    # Practitioner roster. gold_build assigns fte=0.6 to every 4th practitioner
    # by index; if the generator hands them a full caseload their utilisation
    # exceeds 100%, which looks like a bug rather than a busy clinic. The two
    # must agree, so the weighting is mirrored here.
    PRACTITIONERS = list(range(10, 22))
    PRAC_WEIGHTS = [0.6 if i % 4 == 0 else 1.0 for i in range(len(PRACTITIONERS))]

    now = datetime.now(timezone.utc)
    out = []

    for i in range(PAGE_SIZE):
        n = (page - 1) * PAGE_SIZE + i

        # Long tail: a third of appointments belong to a small, frequently
        # seen cohort. Chronic caseloads look like this, and it is what makes
        # lifetime value worth banding at all.
        if random.random() < 0.35:
            patient_no = random.randint(0, SAMPLE_PATIENTS // 8)
        else:
            patient_no = random.randint(0, SAMPLE_PATIENTS - 1)

        offset = random.randint(-SAMPLE_DAYS_BACK, SAMPLE_DAYS_FORWARD)
        start = (now + timedelta(days=offset)).replace(
            hour=random.randint(8, 17), minute=random.choice([0, 15, 30, 45]),
            second=0, microsecond=0,
        )

        # Clinics are quiet over the Australian summer holidays. Dropping most
        # January rows gives the trend charts a visible seasonal dip.
        if start.month == 1 and random.random() < 0.55:
            continue

        duration = random.choice(durations)
        is_future = start > now

        if is_future:
            # Nothing in the future has happened yet.
            cancelled, dna, arrived = random.random() < 0.04, False, False
        else:
            cancelled = random.random() < 0.11
            dna = (not cancelled) and random.random() < 0.09
            arrived = (not cancelled) and (not dna) and random.random() < 0.94

        # Modified shortly after the appointment, or shortly after booking for
        # future ones. Spread matters: the Silver dedupe orders by this, and
        # with every record modified at the same instant that path is never
        # exercised.
        modified = (now if is_future else start) + timedelta(
            minutes=random.randint(5, 2880)
        )
        modified = min(modified, now)

        out.append({
            "ID": 100000 + n,
            "patientID": 5000 + patient_no,
            "practitionerID": random.choices(PRACTITIONERS, weights=PRAC_WEIGHTS)[0],
            "locationID": 1 + (n % 6),
            "appointmentDate": start.date().isoformat(),
            "appointmentStartTime": start.strftime("%H:%M"),
            "appointmentEndTime": (start + timedelta(minutes=duration)).strftime("%H:%M"),
            "appointmentType": random.choice(["Initial", "Standard", "Extended"]),
            "appointmentTypeID": 1 + (n % 6),
            "arrived": "1" if arrived else "0",
            "invoiceGenerated": "1" if arrived else "0",
            "emailReminderSent": random.choice(["0", "1"]),
            "DNA": "1" if dna else "0",
            "cancelled": "1" if cancelled else "0",
            "cancellationDate": start.date().isoformat() if cancelled else None,
            "Notes": random.choice(
                [None, None, None, "patient called, sick", "car broke down",
                 "double booked by mistake", "work conflict"]
            ),
            "patient_first_name": first[patient_no % len(first)],
            "patient_last_name": last[patient_no % len(last)],
            "patient_email": random.choice(
                [f"user{patient_no}@example.com",
                 f"User{patient_no}@Example.COM",
                 None]
            ),
            "patient_phone": random.choice(phone_formats).format(n=patient_no % 1000),
            "patient_dob": (date(1940, 1, 1) + timedelta(days=(patient_no * 37) % 25000)).isoformat(),
            "postcode": f"2{(patient_no % 300):03d}",
            "lastModified": modified.strftime("%Y-%m-%d %H:%M:%S"),
            "dateCreated": (start - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S"),
        })

    # Deliberate duplicates with a LATER lastModified, so last-writer-wins in
    # Silver has something real to resolve.
    for dup in out[:5]:
        later = dict(dup)
        later["lastModified"] = now.strftime("%Y-%m-%d %H:%M:%S")
        later["cancelled"] = "1"
        later["arrived"] = "0"
        out.append(later)

    return out, (page + 1 if page < SAMPLE_PAGES else None)


def _nookal_post(path: str, payload: dict) -> dict:
    """One POST to Nookal, with retry and backoff. Returns the parsed envelope.

    Transport per the SDK: POST, form-encoded body, api_key as a body field.
    Note `data=` and NOT `params=` — params would put everything in the query
    string, the server would see no api_key, and the error would talk about
    authentication rather than about the mistake.
    """
    import requests

    api_key = dbutils.secrets.get(scope="clinic", key="nookal_api_key")
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
