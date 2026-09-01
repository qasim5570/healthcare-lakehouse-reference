# Databricks notebook source
# ---------------------------------------------------------------------------
# Quality gate.
#
# Expectations catch malformed data. This catches WRONG data, which is what
# actually destroys credibility with a finance team. It runs after Silver and
# before anything publishes, and it raises rather than reporting: a failed gate
# should leave yesterday's Gold in place rather than publishing today's bad
# numbers.
#
# Each check returns (name, severity, value, threshold, passed).
# ---------------------------------------------------------------------------

from pyspark.sql import functions as F
from avanti.params import param

CATALOG = param("catalog")
SILVER = param("silver_schema")

fact = spark.table(f"{CATALOG}.{SILVER}.fct_appointment")
total = fact.count()

results = []


def check(name: str, severity: str, value: float, threshold: float, passed: bool) -> None:
    results.append((name, severity, float(value), float(threshold), passed))


# COMMAND ----------

# FAIL checks stop the pipeline. Reserve these for violations that make the
# dataset meaningless.
check("row_count_non_zero", "fail", total, 1, total >= 1)

null_keys = fact.filter(F.col("appointment_key").isNull()).count()
check("no_null_surrogate_keys", "fail", null_keys, 0, null_keys == 0)

dupes = (
    fact.groupBy("appointment_key").count().filter("count > 1").count()
)
check("surrogate_key_unique", "fail", dupes, 0, dupes == 0)

# WARN checks record a metric without stopping the run. Most rules belong here.
unknown_status = fact.filter(F.col("status") == "unknown").count()
unknown_rate = unknown_status / total if total else 0
check("unknown_status_rate", "warn", unknown_rate, 0.05, unknown_rate <= 0.05)

no_phone = fact.filter(F.col("phone_norm").isNull()).count()
phone_null_rate = no_phone / total if total else 0
# A rising rate here degrades the identity match rate, which corrupts every
# cross-domain metric downstream long before anyone notices a dashboard is off.
check("phone_normalisation_rate", "warn", phone_null_rate, 0.30, phone_null_rate <= 0.30)

future_dated = fact.filter(F.col("start_ts") > F.current_timestamp() + F.expr("INTERVAL 365 DAYS")).count()
check("no_implausible_future_dates", "warn", future_dated, 0, future_dated == 0)

# COMMAND ----------

schema = "check_name string, severity string, value double, threshold double, passed boolean"
df = spark.createDataFrame(results, schema).withColumn("run_ts", F.current_timestamp())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops")
df.write.mode("append").saveAsTable(f"{CATALOG}.ops.dq_results")

display(df)

failures = [r for r in results if r[4] is False and r[1] == "fail"]
warnings = [r for r in results if r[4] is False and r[1] == "warn"]

for name, _, value, threshold, _ in warnings:
    print(f"WARN  {name}: {value:.4f} (threshold {threshold})")

if failures:
    detail = ", ".join(f"{n}={v}" for n, _, v, _, _ in failures)
    raise AssertionError(f"Quality gate failed: {detail}")

print(f"Quality gate passed. {len(warnings)} warning(s), {total} rows checked.")
