# Databricks notebook source
# ---------------------------------------------------------------------------
# Landing volume -> Bronze.
#
# This is the boundary between files and tables. Auto Loader tracks which files
# it has already consumed (RocksDB in the checkpoint), so reruns are safe and
# nothing is processed twice.
#
# Bronze is append-only and never edited. It is the replay tape: if a parsing
# bug corrupts Silver, you rebuild from here rather than re-hitting a
# rate-limited API for three years of history.
# ---------------------------------------------------------------------------

from pyspark.sql import functions as F
from lakehouse.params import param

CATALOG = param("catalog")
LANDING = param("landing_schema")
BRONZE = param("bronze_schema")
SOURCE = param("source")
ENTITY = param("entity")
OPS = param("ops_schema")

TABLE = f"{CATALOG}.{BRONZE}.{SOURCE}_{ENTITY}"
SOURCE_PATH = f"/Volumes/{CATALOG}/{LANDING}/raw/{SOURCE}/{ENTITY}"
CHECKPOINT = f"/Volumes/{CATALOG}/{OPS}/checkpoints/{SOURCE}_{ENTITY}"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE}")

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    # 'rescue' means an unexpected field lands in _rescued_data instead of
    # failing the run or being dropped. Alert when it is non-null: that is your
    # early warning for an upstream schema change.
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/schema")
    .load(SOURCE_PATH)
    .withColumn("_source", F.lit(SOURCE))
    .withColumn("_entity", F.lit(ENTITY))
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn(
        "_batch_id",
        F.regexp_extract(F.col("_metadata.file_path"), r"/([0-9a-f\-]{36})_", 1),
    )
)

# Keep the original payload. It costs almost nothing and saves you every time a
# field turns out to matter six months later.
payload_cols = [c for c in stream.columns if not c.startswith("_")]
stream = (
    stream
    .withColumn("_raw_payload", F.to_json(F.struct(*payload_cols)))
    .withColumn("_payload_hash", F.sha2(F.col("_raw_payload"), 256))
)

# COMMAND ----------

(
    stream.writeStream
    .option("checkpointLocation", f"{CHECKPOINT}/write")
    .option("mergeSchema", "true")
    # availableNow processes everything currently waiting, then stops. Cheaper
    # than a continuously running stream, and correct for a scheduled job.
    .trigger(availableNow=True)
    .toTable(TABLE)
    .awaitTermination()
)

count = spark.table(TABLE).count()
rescued = spark.table(TABLE).filter("_rescued_data IS NOT NULL").count()

print(f"{TABLE}: {count} rows total")
if rescued:
    print(f"WARNING: {rescued} rows carry _rescued_data - the source schema changed")
