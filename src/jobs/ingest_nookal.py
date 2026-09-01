# Databricks notebook source
# ---------------------------------------------------------------------------
# Extract -> landing volume.
#
# This is the ONLY place plain Python runs. The bottleneck is a rate-limited
# HTTP endpoint, not compute, so there is nothing for Spark to do here. Do not
# fan API calls across executors: you get throttled, partial extractions become
# hard to reason about, and token refresh stops being deterministic.
#
# mode=sample generates realistic fake data so the whole chain is runnable
# before Nookal credentials arrive. mode=api is the real path.
# ---------------------------------------------------------------------------

import json
import os
import random
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(".."))
from avanti.transforms import landing_path  # noqa: E402
from avanti.params import param

CATALOG = param("catalog")
SCHEMA = param("landing_schema")
SOURCE = param("source")
ENTITY = param("entity")
MODE = param("mode", allowed={"sample", "api"})

RUN_DATE = date.today().isoformat()
BATCH_ID = str(uuid.uuid4())

# COMMAND ----------


def generate_sample_page(page_num: int, rows: int = 200) -> list[dict]:
    """Stand-in for the Nookal API until credentials arrive.

    Deliberately messy: inconsistent phone formats, mixed-case emails, a few
    unrecognised statuses and some missing fields. Clean sample data would let
    bugs through that real data would catch.
    """
    statuses = ["completed", "Completed", "cancelled", "DNA", "no show",
                "booked", "rescheduled", "attended", "wibble"]
    phone_formats = ["0412 345 {n:03d}", "+61 412 345 {n:03d}", "0412345{n:03d}",
                     "(04) 1234 5{n:03d}", "61412345{n:03d}"]
    first = ["Sarah", "James", "Priya", "Wei", "Mohammed", "Emma", "Liam", "Aroha"]
    last = ["Mitchell", "Nguyen", "Patel", "Chen", "Okafor", "Wilson", "Brown"]

    out = []
    for i in range(rows):
        n = page_num * rows + i
        start = datetime.now(timezone.utc) - timedelta(
            days=random.randint(0, 120), hours=random.randint(8, 17)
        )
        out.append({
            "id": 100000 + n,
            "patient_id": 5000 + (n % 900),
            "practitioner_id": 10 + (n % 12),
            "location_id": 1 + (n % 6),
            "appointment_date": start.replace(microsecond=0).isoformat(),
            "duration": random.choice([15, 30, 30, 45, 60]),
            "status": random.choice(statuses),
            "cancellation_reason": random.choice(
                [None, None, None, "patient called, sick", "car broke down",
                 "double booked by mistake", "work conflict"]
            ),
            "patient_first_name": random.choice(first),
            "patient_last_name": random.choice(last),
            "patient_email": random.choice(
                [f"user{n % 900}@example.com", f"User{n % 900}@Example.COM", None]
            ),
            "patient_phone": random.choice(phone_formats).format(n=n % 1000),
            "patient_dob": (date(1950, 1, 1) + timedelta(days=(n * 37) % 20000)).isoformat(),
            "postcode": f"2{(n % 900):03d}",
            "last_modified": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        })
    return out


def fetch_api_page(page_num: int) -> list[dict]:
    """Real extraction. Fill in once credentials exist.

    Non-negotiables when you wire this up:
      - read the key from a secret scope, never inline
      - read the watermark from ops.watermark and commit it only after a
        successful full entity pull
      - overlap the window by 24-48h; SaaS updated_at fields are not reliably
        transactional, and Silver dedupes anyway
      - retry 429/5xx with exponential backoff plus jitter, honour Retry-After
    """
    raise NotImplementedError(
        "Set mode=sample until Nookal credentials are available. "
        "See README section 'Wiring up a real source'."
    )


# COMMAND ----------

target_dir = landing_path(CATALOG, SCHEMA, SOURCE, ENTITY, RUN_DATE)
dbutils.fs.mkdirs(target_dir)

pages = 3
total_records = 0

for page_num in range(pages):
    records = (generate_sample_page(page_num) if MODE == "sample"
               else fetch_api_page(page_num))
    if not records:
        break

    # Idempotent filename: a retry of the same batch overwrites rather than
    # duplicating, and Auto Loader's file tracking handles the rest.
    file_path = f"{target_dir}/{BATCH_ID}_{page_num:05d}.json"
    dbutils.fs.put(file_path, "\n".join(json.dumps(r) for r in records), overwrite=True)
    total_records += len(records)

print(f"landed {total_records} records across {pages} pages")
print(f"batch_id: {BATCH_ID}")
print(f"path:     {target_dir}")

dbutils.notebook.exit(json.dumps({
    "batch_id": BATCH_ID,
    "records": total_records,
    "path": target_dir,
}))
