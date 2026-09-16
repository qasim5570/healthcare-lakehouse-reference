# Databricks notebook source
# ---------------------------------------------------------------------------
# Bronze -> Silver: clean, identify, conform.
#
#   1. parse, type, deduplicate (last writer wins)
#   2. normalise identifiers into a candidate pool
#   3. emit the fact table with deterministic surrogate keys
#
# NAMING: Silver tables are plain nouns — `appointment`, not `fct_appointment`.
# Silver is CONFORMED SOURCE DATA, not a dimensional model. The fct_/dim_
# prefixes belong in Gold, where they carry meaning.
#
# Column names here match Nookal's ACTUAL API response, read from the PHP SDK
# (types/Appointments.php): ID, patientID, practitionerID, locationID,
# appointmentDate, appointmentStartTime, DNA, cancelled, lastModified. Note the
# inconsistent casing — that is the vendor's, and Bronze preserves it verbatim.
# ---------------------------------------------------------------------------

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from avanti.params import param
from avanti.transforms import (
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

udf_email = F.udf(normalise_email, StringType())
udf_phone = F.udf(normalise_phone, StringType())
udf_name_key = F.udf(name_key, StringType())
udf_key = F.udf(lambda src, nid: surrogate_key(src, nid), StringType())

# COMMAND ----------

bronze = spark.table(f"{CATALOG}.{BRONZE}.nookal_appointments")

# Nookal has NO single status column — it has three independent flags. Order
# matters: a cancelled appointment can also carry DNA, and cancelled wins.
status_expr = (
    F.when(F.col("cancelled") == "1", F.lit("cancelled"))
    .when(F.col("DNA") == "1", F.lit("dna"))
    .when(F.col("arrived") == "1", F.lit("completed"))
    .otherwise(F.lit("booked"))
)

# appointmentDate is a DATE and appointmentStartTime a TIME, in separate fields.
start_ts_expr = F.to_timestamp(
    F.concat_ws(" ", F.col("appointmentDate"), F.col("appointmentStartTime"))
)
end_ts_expr = F.to_timestamp(
    F.concat_ws(" ", F.col("appointmentDate"), F.col("appointmentEndTime"))
)

# Nookal gives no duration field; derive it from the two times.
duration_expr = (
    (F.unix_timestamp(end_ts_expr) - F.unix_timestamp(start_ts_expr)) / 60
).cast("int")

typed = bronze.select(
    F.col("ID").cast("string").alias("src_appointment_id"),
    F.col("patientID").cast("string").alias("src_patient_id"),
    F.col("practitionerID").cast("string").alias("src_practitioner_id"),
    F.col("locationID").cast("string").alias("src_clinic_id"),
    F.col("appointmentTypeID").cast("string").alias("src_service_id"),
    F.col("appointmentType").alias("service_name"),

    # Stored in UTC. Local calendar dates are derived downstream via dim_date,
    # never with an inline cast on a UTC column.
    start_ts_expr.alias("start_ts"),
    end_ts_expr.alias("end_ts"),
    duration_expr.alias("duration_min"),

    status_expr.alias("status"),
    F.col("cancelled").alias("flag_cancelled_raw"),
    F.col("DNA").alias("flag_dna_raw"),
    F.col("arrived").alias("flag_arrived_raw"),
    F.to_date("cancellationDate").alias("cancellation_date"),
    F.col("Notes").alias("notes"),
    F.col("invoiceGenerated").alias("flag_invoiced_raw"),

    # Patient detail. NOTE: these are NOT real getAppointments fields — the
    # sample generator emits them for convenience. See the warning below.
    F.col("patient_first_name"),
    F.col("patient_last_name"),
    udf_email(F.col("patient_email")).alias("email_norm"),
    udf_phone(F.col("patient_phone")).alias("phone_norm"),
    udf_name_key(F.col("patient_first_name"), F.col("patient_last_name")).alias("name_key"),
    F.to_date("patient_dob").alias("date_of_birth"),
    F.col("postcode"),

    F.to_timestamp("lastModified").alias("_source_updated_ts"),
    F.to_timestamp("dateCreated").alias("_source_created_ts"),
    F.col("_ingest_ts"),
    F.col("_batch_id"),
)

# Last-writer-wins deduplication. The 48h overlap window on extraction means
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

appointment = (
    deduped
    .filter(F.col("src_appointment_id").isNotNull() & F.col("start_ts").isNotNull())
    .withColumn("appointment_key", udf_key(F.lit("nookal"), F.col("src_appointment_id")))
    .withColumn("patient_key", udf_key(F.lit("nookal"), F.col("src_patient_id")))
    .withColumn("clinic_key", udf_key(F.lit("nookal"), F.col("src_clinic_id")))
    .withColumn("practitioner_key", udf_key(F.lit("nookal"), F.col("src_practitioner_id")))
    .withColumn("is_completed", F.col("status") == "completed")
    .withColumn("is_dna", F.col("status") == "dna")
    .withColumn("is_cancelled", F.col("status") == "cancelled")
    # Source-side deletions never appear in an incremental pull, so every fact
    # carries this flag and the weekly sweep sets it. Gold filters on it.
    .withColumn("is_deleted", F.lit(False))
    .withColumn("deleted_detected_at", F.lit(None).cast("timestamp"))
    .drop("patient_first_name", "patient_last_name", "email_norm",
          "phone_norm", "name_key", "date_of_birth", "postcode")
)

appointment.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SILVER}.appointment"
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# WARNING — party_candidate is built from the WRONG entity.
#
# Real getAppointments returns NO patient detail: no name, email, phone, dob or
# postcode. Those live on getPatients. The sample generator emits them on the
# appointment record for convenience, which is why this works today and will
# break the moment mode=api is used.
#
# CORRECT DESIGN: a separate nookal.patients extractor feeding a separate
# Silver job that builds party_candidate. Appointments contribute patientID
# only, as a foreign key.
#
# Left in place so the identity spine has something to work with in the
# sandbox. Do NOT carry this into Avanti's build.
# ---------------------------------------------------------------------------
(
    deduped.select(
        F.lit("nookal").alias("source"),
        F.col("src_patient_id").alias("source_party_id"),
        "email_norm", "phone_norm", "name_key", "date_of_birth", "postcode",
    )
    .dropDuplicates(["source", "source_party_id"])
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER}.party_candidate")
)

print(f"appointment:      {spark.table(f'{CATALOG}.{SILVER}.appointment').count()} rows")
print(f"party_candidate:  {spark.table(f'{CATALOG}.{SILVER}.party_candidate').count()} rows")
