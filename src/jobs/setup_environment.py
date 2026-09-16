# Databricks notebook source
# ---------------------------------------------------------------------------
# Environment setup — runs scripts/setup_catalog.sql as a job.
#
# WHY THIS EXISTS
#   Pasting 13 DDL statements into the SQL editor works once. It does not
#   survive being done three times (dev/test/prod), does not tell you WHICH
#   statement failed, and is not version controlled.
#
#   This makes environment creation the same thing as everything else: a
#   deployed, parameterised, idempotent job.
#
# IDEMPOTENT
#   Every statement is CREATE ... IF NOT EXISTS, so re-running is safe and a
#   no-op. Use it to bring an existing environment up to the current schema
#   definition after the SQL file changes.
#
# PREREQUISITE — the catalog itself must exist first, bound to your storage:
#
#     CREATE CATALOG IF NOT EXISTS avanti_dev
#     MANAGED LOCATION 'abfss://<container>@<account>.dfs.core.windows.net/';
#
#   That one statement stays manual: it names the storage path, it is
#   genuinely once-per-environment, and getting it wrong silently puts your
#   data on Databricks-managed storage instead of your own.
# ---------------------------------------------------------------------------

import re
import traceback

from avanti.params import param

CATALOG = param("catalog")
SQL_PATH = param("sql_path")        # workspace path to setup_catalog.sql

print(f"catalog  : {CATALOG}")
print(f"sql file : {SQL_PATH}")

# COMMAND ----------


def load_statements(path: str, catalog: str) -> list[str]:
    """Read a .sql file and return executable statements.

    Splitting naively on ';' breaks on semicolons inside comments, so comments
    are stripped FIRST. Line comments only — the file uses no block comments.
    """
    with open(path) as f:
        raw = f.read()

    no_comments = "\n".join(
        line for line in raw.splitlines()
        if not line.strip().startswith("--")
    )

    out = []
    for stmt in no_comments.split(";"):
        stmt = stmt.strip()
        if stmt:
            out.append(stmt.replace("${catalog}", catalog))
    return out


def first_line(stmt: str, width: int = 78) -> str:
    """A one-line label for a statement, for the progress log."""
    flat = re.sub(r"\s+", " ", stmt).strip()
    return flat[:width] + ("…" if len(flat) > width else "")


statements = load_statements(SQL_PATH, CATALOG)
print(f"\n{len(statements)} statements to execute\n")

# COMMAND ----------

results = []
failed = 0

for i, stmt in enumerate(statements, start=1):
    label = first_line(stmt)
    try:
        spark.sql(stmt)
        results.append((i, "OK", label, ""))
        print(f"  [{i:>2}/{len(statements)}] OK    {label}")
    except Exception as exc:
        # Keep going rather than stopping at the first failure — one run
        # should tell you about ALL the problems, not just the first.
        msg = re.sub(r"\s+", " ", str(exc))[:200]
        results.append((i, "FAILED", label, msg))
        failed += 1
        print(f"  [{i:>2}/{len(statements)}] FAIL  {label}")
        print(f"          -> {msg}")

print(f"\n{len(statements) - failed} succeeded, {failed} failed")

# A rendered table, so the run page shows a summary rather than a wall of text.
display(
    spark.createDataFrame(results, "seq INT, status STRING, statement STRING, error STRING")
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# VERIFICATION — the checks that used to be commented out at the bottom of the
# SQL file, now actually run, with their results side by side.
# ---------------------------------------------------------------------------

EXPECTED_SCHEMAS = {"landing", "bronze", "silver", "gold", "gold_secure", "ops", "ml"}
EXPECTED_OPS_TABLES = {"watermark", "ingest_audit", "dq_results", "recon_results"}

checks = []

schemas = {r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()}
missing = EXPECTED_SCHEMAS - schemas
checks.append(("schemas", "OK" if not missing else "MISSING",
               ", ".join(sorted(missing)) or f"{len(EXPECTED_SCHEMAS)} present"))

landing_vols = {r.volume_name for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.landing").collect()}
checks.append(("landing.raw volume", "OK" if "raw" in landing_vols else "MISSING",
               ", ".join(sorted(landing_vols))))

ops_vols = {r.volume_name for r in spark.sql(f"SHOW VOLUMES IN {CATALOG}.ops").collect()}
checks.append(("ops.checkpoints volume", "OK" if "checkpoints" in ops_vols else "MISSING",
               ", ".join(sorted(ops_vols))))

ops_tables = {r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.ops").collect()}
missing_t = EXPECTED_OPS_TABLES - ops_tables
checks.append(("ops tables", "OK" if not missing_t else "MISSING",
               ", ".join(sorted(missing_t)) or f"{len(EXPECTED_OPS_TABLES)} present"))

# The one that catches a silent misconfiguration: if MANAGED LOCATION was
# omitted at CREATE CATALOG, everything above still succeeds but the data
# lands on Databricks-managed storage instead of your own container.
detail = {r.info_name: r.info_value
          for r in spark.sql(f"DESCRIBE CATALOG EXTENDED {CATALOG}").collect()}
storage = detail.get("Storage Root") or detail.get("Storage Location") or "(not reported)"
own_storage = storage.startswith("abfss://")
checks.append(("catalog storage root",
               "OK" if own_storage else "CHECK — not an abfss:// path",
               storage))

display(spark.createDataFrame(checks, "check STRING, status STRING, detail STRING"))

# COMMAND ----------

bad = [c for c in checks if c[1] != "OK"]
if failed or bad:
    raise RuntimeError(
        f"setup incomplete: {failed} statement(s) failed, "
        f"{len(bad)} verification check(s) not OK — see the tables above"
    )

print(f"{CATALOG} is ready.")
print(f"storage root: {storage}")
