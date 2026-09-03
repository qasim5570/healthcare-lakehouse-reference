"""Watermark and audit-log helpers for extractors.

These two mechanisms are what separate a working extractor from a demo:

  WATERMARK  — where did I get to last time? Without it, every run either
               re-extracts everything or silently skips records.

  AUDIT      — did last night's run actually work? A job can "succeed" while
               pulling zero rows for three days. The audit table is what you
               show the business when someone asks whether a number is stale.

Both live in the `ops` schema, deliberately separate from business data:
different grants, different lifecycle, and a Bronze full refresh must never take
the audit trail with it.

Needs Spark, so this is not unit-testable the way transforms.py is.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from databricks.sdk.runtime import spark


# How far back to look on the very first run, when no watermark exists yet.
DEFAULT_BACKFILL_DAYS = 730

# SaaS `updated_at` fields are not reliably transactional: a record can be
# committed slightly after its own timestamp, so a strict "> watermark" query
# misses it forever. Re-reading a small overlap and letting Silver deduplicate
# is far cheaper than losing records.
OVERLAP_HOURS = 48


def read_watermark(catalog: str, ops_schema: str, source: str, entity: str) -> datetime:
    """Return the timestamp to extract FROM, overlap already applied.

    First run for an entity returns now minus DEFAULT_BACKFILL_DAYS.
    """
    rows = spark.sql(
        f"""
        SELECT high_water_mark
        FROM   {catalog}.{ops_schema}.watermark
        WHERE  source = '{source}' AND entity = '{entity}'
        """
    ).collect()

    if not rows or rows[0]["high_water_mark"] is None:
        start = datetime.now(timezone.utc) - timedelta(days=DEFAULT_BACKFILL_DAYS)
        print(f"[watermark] no watermark for {source}.{entity}; backfilling from {start:%Y-%m-%d}")
        return start

    hwm = rows[0]["high_water_mark"]
    if hwm.tzinfo is None:
        hwm = hwm.replace(tzinfo=timezone.utc)

    start = hwm - timedelta(hours=OVERLAP_HOURS)
    print(f"[watermark] {source}.{entity} last at {hwm:%Y-%m-%d %H:%M}, "
          f"extracting from {start:%Y-%m-%d %H:%M} ({OVERLAP_HOURS}h overlap)")
    return start


def commit_watermark(
    catalog: str,
    ops_schema: str,
    source: str,
    entity: str,
    new_high_water: datetime,
    batch_id: str,
) -> None:
    """Advance the watermark. CALL THIS ONLY AFTER A FULLY SUCCESSFUL PULL.

    If page 7 of 20 fails, the watermark must stay where it was so the next run
    re-reads from the old position. Committing on partial success is how records
    get silently skipped — the run reports success and the gap is invisible
    until someone reconciles counts months later.
    """
    spark.sql(
        f"""
        MERGE INTO {catalog}.{ops_schema}.watermark AS t
        USING (SELECT '{source}' AS source,
                      '{entity}' AS entity,
                      TIMESTAMP '{new_high_water:%Y-%m-%d %H:%M:%S}' AS high_water_mark,
                      '{batch_id}' AS last_batch_id,
                      current_timestamp() AS updated_at) AS s
        ON  t.source = s.source AND t.entity = s.entity
        WHEN MATCHED THEN UPDATE SET
            t.high_water_mark = s.high_water_mark,
            t.last_batch_id   = s.last_batch_id,
            t.updated_at      = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print(f"[watermark] committed {source}.{entity} -> {new_high_water:%Y-%m-%d %H:%M}")


def audit_start(catalog: str, ops_schema: str, source: str, entity: str, batch_id: str) -> None:
    """Record that a run began. Written BEFORE any extraction.

    A row with status RUNNING and no finished_at means the run died without
    completing — which is exactly the state you want visible. If the audit row
    were only written at the end, a crashed run would leave no trace at all.
    """
    spark.sql(
        f"""
        INSERT INTO {catalog}.{ops_schema}.ingest_audit
        VALUES ('{source}', '{entity}', '{batch_id}',
                NULL, NULL, 'RUNNING', NULL,
                current_timestamp(), NULL)
        """
    )


def audit_finish(
    catalog: str,
    ops_schema: str,
    batch_id: str,
    status: str,
    record_count: int | None = None,
    page_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """Close out the audit row: SUCCEEDED or FAILED, with counts."""
    err = "NULL" if error_message is None else "'" + error_message.replace("'", "''")[:1000] + "'"
    rec = "NULL" if record_count is None else str(record_count)
    pag = "NULL" if page_count is None else str(page_count)

    spark.sql(
        f"""
        UPDATE {catalog}.{ops_schema}.ingest_audit
        SET    status = '{status}',
               record_count = {rec},
               page_count = {pag},
               error_message = {err},
               finished_at = current_timestamp()
        WHERE  batch_id = '{batch_id}' AND status = 'RUNNING'
        """
    )
    print(f"[audit] {batch_id} -> {status} ({rec} records, {pag} pages)")
