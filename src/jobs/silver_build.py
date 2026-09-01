# Databricks notebook source
# ---------------------------------------------------------------------------
# Bronze -> Silver: clean, identify, conform.
#
# Three jobs in order:
#   1. parse, type, deduplicate (last writer wins)
#   2. normalise identifiers into a candidate pool
#   3. emit the fact table with deterministic surrogate keys
#
# The normalisation functions come from avanti.transforms so that the exact
# logic under unit test is the logic that runs in production. Duplicating them
# inline here is how the two quietly drift apart.
# ---------------------------------------------------------------------------

import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from avanti.params import param

sys.path.insert(0, os.path.abspath(".."))
from avanti.transforms import (  # noqa: E402
    canonical_status,
    name_key,
    normalise_email,
    normalise_phone,
    surrogate_key,
)

CATALOG = param("catalog")
BRONZE = param("bronze_schema")
SILVER = param("silver_schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}")

# COMMAND ----------

# Wrap the pure functions as UDFs. At this data volume the overhead is
# irrelevant and the testability is worth far more than the microseconds.
udf_email = F.udf(normalise_email, StringType())
udf_phone = F.udf(normalise_phone, StringType())
udf_name_key = F.udf(name_key, StringType())
udf_status = F.udf(canonical_status, StringType())
udf_key = F.udf(lambda src, nid: surrogate_key(src, nid), StringType())

# COMMAND ----------

bronze = spark.table(f"{CATALOG}.{BRONZE}.nookal_appointments")

typed = bronze.select(
    F.col("id").cast("string").alias("src_appointment_id"),
    F.col("patient_id").cast("string").alias("src_patient_id"),
    F.col("practitioner_id").cast("string").alias("src_practitioner_id"),
    F.col("location_id").cast("string").alias("src_clinic_id"),
    # Stored in UTC. Local calendar dates are derived downstream via dim_date,
    # never with an inline cast on a UTC column.
    F.to_timestamp("appointment_date").alias("start_ts"),
    F.col("duration").cast("int").alias("duration_min"),
    udf_status(F.col("status")).alias("status"),
    F.col("status").alias("status_raw"),
    F.col("cancellation_reason"),
    F.col("patient_first_name"),
    F.col("patient_last_name"),
    udf_email(F.col("patient_email")).alias("email_norm"),
    udf_phone(F.col("patient_phone")).alias("phone_norm"),
    udf_name_key(F.col("patient_first_name"), F.col("patient_last_name")).alias("name_key"),
    F.to_date("patient_dob").alias("date_of_birth"),
    F.col("postcode"),
    F.to_timestamp("last_modified").alias("_source_updated_ts"),
    F.col("_ingest_ts"),
    F.col("_batch_id"),
)

# Last-writer-wins deduplication. The 24-48h overlap window on extraction means
# the same record legitimately arrives more than once; this is where that is
# resolved, ordered by the source's own modified timestamp.
w = Window.partitionBy("src_appointment_id").orderBy(
    F.col("_source_updated_ts").desc(), F.col("_ingest_ts").desc()
)
deduped = (
    typed.withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)

# COMMAND ----------

fact = (
    deduped
    .filter(F.col("src_appointment_id").isNotNull() & F.col("start_ts").isNotNull())
    .withColumn("appointment_key", udf_key(F.lit("nookal"), F.col("src_appointment_id")))
    .withColumn("clinic_key", udf_key(F.lit("nookal"), F.col("src_clinic_id")))
    .withColumn("practitioner_key", udf_key(F.lit("nookal"), F.col("src_practitioner_id")))
    .withColumn("is_completed", F.col("status") == "completed")
    .withColumn("is_dna", F.col("status") == "dna")
    .withColumn("is_cancelled", F.col("status") == "cancelled")
    # Source-side deletions never appear in an incremental pull, so every fact
    # carries this flag and the weekly sweep sets it. Gold filters on it.
    .withColumn("is_deleted", F.lit(False))
    .withColumn("deleted_detected_at", F.lit(None).cast("timestamp"))
)

fact.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SILVER}.fct_appointment"
)

# COMMAND ----------

# Candidate pool for the identity spine. With one source this is trivial; it
# becomes the load-bearing asset once Hapana, AlayaCare and GHL land and the
# same human exists four times over with four unrelated ids.
(
    deduped.select(
        F.lit("nookal").alias("source"),
        F.col("src_patient_id").alias("source_party_id"),
        "email_norm", "phone_norm", "name_key", "date_of_birth", "postcode",
    )
    .dropDuplicates(["source", "source_party_id"])
    .write.mode("overwrite")
    .saveAsTable(f"{CATALOG}.{SILVER}.party_candidate")
)

print(f"fct_appointment:  {spark.table(f'{CATALOG}.{SILVER}.fct_appointment').count()} rows")
print(f"party_candidate:  {spark.table(f'{CATALOG}.{SILVER}.party_candidate').count()} rows")
