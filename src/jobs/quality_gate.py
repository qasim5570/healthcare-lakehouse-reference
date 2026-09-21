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
# fail = the dataset is meaningless, stop the pipeline.
# warn = record the metric, keep going. Most rules belong here.
#
# The value is in TRENDING these, not just alerting. A phone-normalisation
# failure rate creeping from 8% to 20% over a month is a real problem that no
# single run would flag.
# ---------------------------------------------------------------------------

from pyspark.sql import functions as F

from lakehouse.params import param

CATALOG = param("catalog")
SILVER = param("silver_schema")
OPS = param("ops_schema")

results = []


def check(name: str, severity: str, value: float, threshold: float, passed: bool) -> None:
    results.append((name, severity, float(value), float(threshold), passed))


def rate(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


# COMMAND ----------

# ---------------------------------------------------------------------------
# silver.appointment
# ---------------------------------------------------------------------------
appt = spark.table(f"{CATALOG}.{SILVER}.appointment")
total = appt.count()

check("row_count_non_zero", "fail", total, 1, total >= 1)

null_keys = appt.filter(F.col("appointment_key").isNull()).count()
check("no_null_surrogate_keys", "fail", null_keys, 0, null_keys == 0)

dupes = appt.groupBy("appointment_key").count().filter("count > 1").count()
check("surrogate_key_unique", "fail", dupes, 0, dupes == 0)

orphan_clinic = appt.filter(F.col("clinic_key").isNull()).count()
check("clinic_key_present", "warn", rate(orphan_clinic, total), 0.01,
      rate(orphan_clinic, total) <= 0.01)

# Nookal has no status STRING — status is derived from the cancelled / DNA /
# arrived flags, so nothing can map to "unknown". A high 'booked' rate instead
# means the flags are not being set as expected upstream.
booked = appt.filter(F.col("status") == "booked").count()
check("booked_status_rate", "warn", rate(booked, total), 0.60,
      rate(booked, total) <= 0.60)

future_dated = appt.filter(
    F.col("start_ts") > F.current_timestamp() + F.expr("INTERVAL 365 DAYS")
).count()
check("no_implausible_future_dates", "warn", future_dated, 0, future_dated == 0)

bad_duration = appt.filter(
    F.col("duration_min").isNull() | (F.col("duration_min") <= 0) | (F.col("duration_min") > 480)
).count()
check("duration_plausible", "warn", rate(bad_duration, total), 0.05,
      rate(bad_duration, total) <= 0.05)

# COMMAND ----------

# ---------------------------------------------------------------------------
# party_candidate — identifier normalisation lives here, because
# silver.appointment no longer carries patient detail.
# ---------------------------------------------------------------------------
party = spark.table(f"{CATALOG}.{SILVER}.party_candidate")
party_total = party.count()

check("party_count_non_zero", "fail", party_total, 1, party_total >= 1)

no_phone = party.filter(F.col("phone_norm").isNull()).count()
# A rising rate here degrades the identity match rate, which corrupts every
# cross-domain metric downstream long before anyone notices a dashboard is off.
check("phone_normalisation_rate", "warn", rate(no_phone, party_total), 0.30,
      rate(no_phone, party_total) <= 0.30)

no_email = party.filter(F.col("email_norm").isNull()).count()
check("email_normalisation_rate", "warn", rate(no_email, party_total), 0.40,
      rate(no_email, party_total) <= 0.40)

# A party with no phone, no email and no DOB cannot be matched to any other
# source. This rate is the hard CEILING on the eventual identity match rate —
# worth knowing before promising any cross-domain attribution.
unmatchable = party.filter(
    F.col("phone_norm").isNull()
    & F.col("email_norm").isNull()
    & F.col("date_of_birth").isNull()
).count()
check("party_has_matchable_identifier", "warn", rate(unmatchable, party_total), 0.10,
      rate(unmatchable, party_total) <= 0.10)

dupe_party = party.groupBy("source", "source_party_id").count().filter("count > 1").count()
check("party_id_unique_per_source", "fail", dupe_party, 0, dupe_party == 0)

# COMMAND ----------

schema = "check_name string, severity string, value double, threshold double, passed boolean"
df = spark.createDataFrame(results, schema).withColumn("run_ts", F.current_timestamp())

df.write.mode("append").saveAsTable(f"{CATALOG}.{OPS}.dq_results")

display(df.orderBy(F.col("passed").asc(), "severity", "check_name"))

failures = [r for r in results if r[4] is False and r[1] == "fail"]
warnings = [r for r in results if r[4] is False and r[1] == "warn"]

for name, _, value, threshold, _ in warnings:
    print(f"WARN  {name}: {value:.4f} (threshold {threshold})")

if failures:
    detail = ", ".join(f"{n}={v}" for n, _, v, _, _ in failures)
    raise AssertionError(f"Quality gate FAILED: {detail}")

print(f"Quality gate passed. {len(warnings)} warning(s).")
print(f"  silver.appointment:  {total} rows")
print(f"  party_candidate:  {party_total} rows")
