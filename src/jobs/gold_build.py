# Databricks notebook source
# ---------------------------------------------------------------------------
# Silver -> Gold.
#
# Silver holds appointments and a party pool. That is not enough to answer the
# questions the app needs to answer: there is no revenue, no cost, and no
# capacity denominator, so "total sales" and "utilisation" have no basis.
#
# This job:
#   1. generates the dimensions Nookal does not supply (date, clinic,
#      practitioner, service) from the ids present in the fact
#   2. attaches SYNTHETIC financials — fee, cost, payer mix
#   3. generates practitioner rosters so utilisation has a denominator
#   4. builds the marts the app queries
#
# SYNTHETIC DATA WARNING
#   Revenue, cost and capacity here are GENERATED, not sourced. In the real
#   build these come from Xero (revenue, cost) and Nookal availabilities
#   (capacity). Every generated column is suffixed _synth in Silver so it is
#   obvious at a glance, and this job is the only place they are produced.
#
# Gold is REBUILT each run, not merged. At this volume a full rebuild is
# instant, correct by construction, and needs no state. That stops being true
# somewhere north of a hundred million rows.
# ---------------------------------------------------------------------------

from pyspark.sql import Window
from pyspark.sql import functions as F

from lakehouse.params import param

CATALOG = param("catalog")
SILVER = param("silver_schema")
GOLD = param("gold_schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")

appt = spark.table(f"{CATALOG}.{SILVER}.appointment")
party = spark.table(f"{CATALOG}.{SILVER}.party_candidate")

print(f"silver.appointment: {appt.count()} rows")

# Deterministic — same input produces the same Gold on every rebuild, which is
# what makes a full refresh safe.
SEED = 42


def safe_div(numerator, denominator):
    """Divide, returning NULL rather than raising or producing Infinity.

    A clinic with zero revenue in a month is legitimate (a new site, or a
    closure) and must not blow up the whole mart.
    """
    return F.when(denominator == 0, F.lit(None).cast("double")).otherwise(
        numerator / denominator
    )

# COMMAND ----------

# ---------------------------------------------------------------------------
# dim_date
#
# Every local calendar date is derived HERE, from UTC timestamps, and never
# with an inline cast in a mart. One definition of "what month is this".
# ---------------------------------------------------------------------------
bounds = appt.select(
    F.min(F.to_date("start_ts")).alias("lo"),
    F.max(F.to_date("start_ts")).alias("hi"),
).collect()[0]

dim_date = (
    spark.sql(
        f"SELECT explode(sequence(DATE'{bounds.lo}', DATE'{bounds.hi}', INTERVAL 1 DAY)) AS date_key"
    )
    .withColumn("year", F.year("date_key"))
    .withColumn("month", F.month("date_key"))
    .withColumn("month_start", F.trunc("date_key", "MM"))
    .withColumn("month_label", F.date_format("date_key", "yyyy-MM"))
    .withColumn("quarter", F.concat(F.year("date_key"), F.lit("-Q"), F.quarter("date_key")))
    .withColumn("day_of_week", F.date_format("date_key", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7))
    # Australian financial year: July to June.
    .withColumn(
        "fin_year",
        F.when(F.month("date_key") >= 7, F.concat(F.year("date_key"), F.lit("-"), F.year("date_key") + 1))
        .otherwise(F.concat(F.year("date_key") - 1, F.lit("-"), F.year("date_key"))),
    )
)

dim_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.dim_date"
)
print(f"dim_date: {dim_date.count()} days")

# COMMAND ----------

# ---------------------------------------------------------------------------
# dim_clinic — six clinics, names and attributes generated
# ---------------------------------------------------------------------------
CLINIC_NAMES = ["Bondi Junction", "Parramatta", "Chatswood",
                "Newtown", "Liverpool", "Manly"]
CLINIC_STATES = ["NSW"] * 6
CLINIC_ROOMS = [6, 8, 5, 4, 7, 5]

clinic_ids = sorted(r.src_clinic_id for r in
                    appt.select("src_clinic_id").distinct().collect() if r.src_clinic_id)

dim_clinic = spark.createDataFrame(
    [
        (
            cid,
            CLINIC_NAMES[i % len(CLINIC_NAMES)],
            CLINIC_STATES[i % len(CLINIC_STATES)],
            CLINIC_ROOMS[i % len(CLINIC_ROOMS)],
            "Australia/Sydney",
        )
        for i, cid in enumerate(clinic_ids)
    ],
    "src_clinic_id string, clinic_name string, state string, "
    "treatment_rooms int, clinic_timezone string",
).withColumn("clinic_key", F.sha2(F.concat(F.lit("nookal||"), F.col("src_clinic_id")), 256).substr(1, 32))

dim_clinic.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.dim_clinic"
)
print(f"dim_clinic: {dim_clinic.count()} clinics")

# COMMAND ----------

# ---------------------------------------------------------------------------
# dim_practitioner — discipline and cost rate drive the margin calculation
# ---------------------------------------------------------------------------
DISCIPLINES = [
    ("Physiotherapy", 95.0),
    ("Podiatry", 88.0),
    ("Exercise Physiology", 78.0),
    ("Occupational Therapy", 92.0),
    ("Dietetics", 82.0),
]
FIRST = ["Sarah", "James", "Priya", "Wei", "Mohammed", "Emma",
         "Liam", "Aroha", "Daniel", "Sophie", "Raj", "Chloe"]
LAST = ["Mitchell", "Nguyen", "Patel", "Chen", "Okafor", "Wilson",
        "Brown", "Taylor", "Singh", "Kaur", "Novak", "Ferrari"]

prac_ids = sorted(r.src_practitioner_id for r in
                  appt.select("src_practitioner_id").distinct().collect() if r.src_practitioner_id)

dim_practitioner = spark.createDataFrame(
    [
        (
            pid,
            f"{FIRST[i % len(FIRST)]} {LAST[i % len(LAST)]}",
            DISCIPLINES[i % len(DISCIPLINES)][0],
            float(DISCIPLINES[i % len(DISCIPLINES)][1]),
            clinic_ids[i % len(clinic_ids)] if clinic_ids else None,
            1.0 if i % 4 else 0.6,       # FTE — a quarter are part time
        )
        for i, pid in enumerate(prac_ids)
    ],
    "src_practitioner_id string, practitioner_name string, discipline string, "
    "hourly_cost_synth double, home_clinic_id string, fte double",
).withColumn(
    "practitioner_key",
    F.sha2(F.concat(F.lit("nookal||"), F.col("src_practitioner_id")), 256).substr(1, 32),
)

dim_practitioner.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.dim_practitioner"
)
print(f"dim_practitioner: {dim_practitioner.count()} practitioners")

# COMMAND ----------

# ---------------------------------------------------------------------------
# dim_service — fee schedule and payer mix
#
# Payer matters: a DVA or NDIS visit bills differently from a private one, and
# blending them hides which funding stream is actually profitable.
# ---------------------------------------------------------------------------
SERVICES = [
    ("Initial Consultation", 140.0, 45, "Private"),
    ("Standard Consultation", 95.0, 30, "Private"),
    ("Extended Consultation", 165.0, 60, "Private"),
    ("DVA Standard", 88.0, 30, "DVA"),
    ("NDIS Session", 193.99, 60, "NDIS"),
    ("Home Care Visit", 110.0, 45, "HomeCare"),
]

service_ids = sorted(r.src_service_id for r in
                     appt.select("src_service_id").distinct().collect() if r.src_service_id)

dim_service = spark.createDataFrame(
    [
        (
            sid,
            SERVICES[i % len(SERVICES)][0],
            float(SERVICES[i % len(SERVICES)][1]),
            SERVICES[i % len(SERVICES)][2],
            SERVICES[i % len(SERVICES)][3],
        )
        for i, sid in enumerate(service_ids)
    ],
    "src_service_id string, service_name string, standard_fee_synth double, "
    "standard_duration_min int, payer_type string",
)

dim_service.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.dim_service"
)
print(f"dim_service: {dim_service.count()} services")

# COMMAND ----------

# ---------------------------------------------------------------------------
# fct_appointment — the conformed fact, with synthetic money attached
#
# Revenue recognition rules, and they are the kind of thing that MUST be agreed
# with the business rather than assumed:
#   completed -> full fee
#   dna       -> a cancellation fee on private only; DVA and NDIS cannot be billed
#   cancelled -> nothing
#   booked    -> nothing yet, it is in the future
# ---------------------------------------------------------------------------
DNA_FEE_RATE = 0.50          # private DNA billed at half the standard fee


def join_dim(left, right, on: str, keep: list[str]):
    """Join a dimension onto the fact, keeping only the named attributes.

    Guards against AMBIGUOUS_REFERENCE: silver.appointment already carries the
    surrogate keys and the vendor's own service label, so pulling the same
    column names across from a dimension makes every later reference
    ambiguous. Anything the fact already has is dropped from the fact first,
    on the basis that the dimension holds the canonical version.
    """
    selected = right.select(on, *keep)
    clashes = [c for c in keep if c in left.columns]
    if clashes:
        print(f"  join on {on}: dimension overrides {clashes}")
        left = left.drop(*clashes)
    return left.join(selected, on, "left")


joined = appt
joined = join_dim(joined, dim_service, "src_service_id",
                  ["service_name", "standard_fee_synth", "payer_type"])
joined = join_dim(joined, dim_practitioner, "src_practitioner_id",
                  ["practitioner_name", "discipline", "hourly_cost_synth"])
joined = join_dim(joined, dim_clinic, "src_clinic_id",
                  ["clinic_name", "state", "treatment_rooms"])

revenue_expr = (
    F.when(F.col("status") == "completed", F.col("standard_fee_synth"))
    .when(
        (F.col("status") == "dna") & (F.col("payer_type") == "Private"),
        F.col("standard_fee_synth") * F.lit(DNA_FEE_RATE),
    )
    .otherwise(F.lit(0.0))
)

# Practitioner time is consumed whether or not the patient turns up — a DNA
# costs the same as a completed appointment and earns less. That gap is the
# single most actionable number in the clinical mart.
cost_expr = F.when(
    F.col("status").isin("completed", "dna"),
    F.col("hourly_cost_synth") * (F.coalesce(F.col("duration_min"), F.lit(30)) / 60.0),
).otherwise(F.lit(0.0))

# Money columns are computed BEFORE narrowing, because cost_expr depends on
# hourly_cost_synth, which is an input to the calculation rather than an output
# and so is not carried into the fact.
priced = (
    joined
    .withColumn("revenue_synth", F.round(revenue_expr, 2))
    .withColumn("direct_cost_synth", F.round(cost_expr, 2))
    .withColumn("margin_synth", F.round(F.col("revenue_synth") - F.col("direct_cost_synth"), 2))
)

gold_fact = (
    priced
    .filter(~F.col("is_deleted"))
    .select(
        "appointment_key", "patient_key", "clinic_key", "practitioner_key",
        "src_appointment_id", "src_patient_id",
        F.to_date("start_ts").alias("date_key"),
        "start_ts", "end_ts", "duration_min", "status",
        "is_completed", "is_dna", "is_cancelled", "is_deleted",
        "service_name", "payer_type", "standard_fee_synth",
        "practitioner_name", "discipline",
        "clinic_name", "state",
        "revenue_synth", "direct_cost_synth", "margin_synth",
    )
)

gold_fact.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.fct_appointment"
)
print(f"gold.fct_appointment: {gold_fact.count()} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# fct_capacity — the utilisation DENOMINATOR
#
# Without this, "utilisation" is unanswerable: you know appointments booked but
# not hours available. In the real build this comes from Nookal's
# getAppointmentAvailabilities. Here it is generated from FTE.
# ---------------------------------------------------------------------------
# BILLABLE hours, not working hours. A 7.6-hour day is not 7.6 hours of
# appointments — notes, admin, handover and breaks come out of it. Measuring
# utilisation against the full working day understates it and makes every
# practitioner look idle.
#
# 6.5 is an assumption and MUST be agreed with the business: it is the
# denominator of the headline metric, so a change here moves every utilisation
# number on the dashboard.
WEEKDAY_HOURS = 6.5

capacity = (
    dim_practitioner.select("practitioner_key", "src_practitioner_id",
                            "home_clinic_id", "fte", "hourly_cost_synth")
    .crossJoin(dim_date.filter(~F.col("is_weekend")).select("date_key", "month_start", "month_label"))
    .withColumn("available_hours_synth", F.round(F.col("fte") * F.lit(WEEKDAY_HOURS), 2))
    .withColumn(
        "clinic_key",
        F.sha2(F.concat(F.lit("nookal||"), F.col("home_clinic_id")), 256).substr(1, 32),
    )
)

capacity.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.fct_capacity"
)
print(f"gold.fct_capacity: {capacity.count()} practitioner-days")

# COMMAND ----------

# ---------------------------------------------------------------------------
# mart_clinic_monthly — the headline mart. One row per clinic per month.
# ---------------------------------------------------------------------------
booked = (
    gold_fact.join(dim_date.select("date_key", "month_start", "month_label"), "date_key")
    .groupBy("clinic_key", "clinic_name", "state", "month_start", "month_label")
    .agg(
        F.count("*").alias("appointments_total"),
        F.sum(F.col("is_completed").cast("int")).alias("appointments_completed"),
        F.sum(F.col("is_dna").cast("int")).alias("appointments_dna"),
        F.sum(F.col("is_cancelled").cast("int")).alias("appointments_cancelled"),
        F.countDistinct("patient_key").alias("unique_patients"),
        F.round(F.sum("revenue_synth"), 2).alias("revenue"),
        F.round(F.sum("direct_cost_synth"), 2).alias("direct_cost"),
        F.round(F.sum("margin_synth"), 2).alias("margin"),
        F.round(F.sum(F.coalesce("duration_min", F.lit(0))) / 60.0, 2).alias("delivered_hours"),
    )
)

cap_monthly = capacity.groupBy("clinic_key", "month_start").agg(
    F.round(F.sum("available_hours_synth"), 2).alias("available_hours")
)

mart_clinic_monthly = (
    booked.join(cap_monthly, ["clinic_key", "month_start"], "left")
    .withColumn("dna_rate", F.round(safe_div(F.col("appointments_dna"), F.col("appointments_total")), 4))
    .withColumn("cancellation_rate",
                F.round(safe_div(F.col("appointments_cancelled"), F.col("appointments_total")), 4))
    .withColumn("utilisation_pct",
                F.round(safe_div(F.col("delivered_hours"), F.col("available_hours")), 4))
    .withColumn("margin_pct",
                F.round(safe_div(F.col("margin"), F.col("revenue")), 4))
    .withColumn("revenue_per_appointment",
                F.round(safe_div(F.col("revenue"), F.col("appointments_total")), 2))
    # The number worth acting on: revenue forgone to non-attendance.
    .withColumn("revenue_lost_to_dna",
                F.round(F.col("appointments_dna") * F.col("revenue_per_appointment"), 2))
    .orderBy("month_start", "clinic_name")
)

mart_clinic_monthly.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.mart_clinic_monthly"
)
print(f"mart_clinic_monthly: {mart_clinic_monthly.count()} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# mart_practitioner_monthly
# ---------------------------------------------------------------------------
prac_booked = (
    gold_fact.join(dim_date.select("date_key", "month_start", "month_label"), "date_key")
    .groupBy("practitioner_key", "practitioner_name", "discipline",
             "month_start", "month_label")
    .agg(
        F.count("*").alias("appointments_total"),
        F.sum(F.col("is_completed").cast("int")).alias("appointments_completed"),
        F.sum(F.col("is_dna").cast("int")).alias("appointments_dna"),
        F.countDistinct("patient_key").alias("unique_patients"),
        F.round(F.sum("revenue_synth"), 2).alias("revenue"),
        F.round(F.sum("margin_synth"), 2).alias("margin"),
        F.round(F.sum(F.coalesce("duration_min", F.lit(0))) / 60.0, 2).alias("delivered_hours"),
    )
)

prac_cap = capacity.groupBy("practitioner_key", "month_start").agg(
    F.round(F.sum("available_hours_synth"), 2).alias("available_hours")
)

mart_practitioner_monthly = (
    prac_booked.join(prac_cap, ["practitioner_key", "month_start"], "left")
    .withColumn("utilisation_pct",
                F.round(safe_div(F.col("delivered_hours"), F.col("available_hours")), 4))
    .withColumn("dna_rate", F.round(safe_div(F.col("appointments_dna"), F.col("appointments_total")), 4))
    .withColumn("revenue_per_hour",
                F.round(safe_div(F.col("revenue"), F.col("delivered_hours")), 2))
    .orderBy("month_start", F.col("revenue").desc())
)

mart_practitioner_monthly.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.mart_practitioner_monthly"
)
print(f"mart_practitioner_monthly: {mart_practitioner_monthly.count()} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# mart_service_monthly — payer mix. Which funding stream actually pays?
# ---------------------------------------------------------------------------
mart_service_monthly = (
    gold_fact.join(dim_date.select("date_key", "month_start", "month_label"), "date_key")
    .groupBy("service_name", "payer_type", "month_start", "month_label")
    .agg(
        F.count("*").alias("appointments_total"),
        F.sum(F.col("is_completed").cast("int")).alias("appointments_completed"),
        F.round(F.sum("revenue_synth"), 2).alias("revenue"),
        F.round(F.sum("direct_cost_synth"), 2).alias("direct_cost"),
        F.round(F.sum("margin_synth"), 2).alias("margin"),
    )
    .withColumn("margin_pct", F.round(safe_div(F.col("margin"), F.col("revenue")), 4))
    .orderBy("month_start", F.col("revenue").desc())
)

mart_service_monthly.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.mart_service_monthly"
)
print(f"mart_service_monthly: {mart_service_monthly.count()} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# mart_patient_summary — retention and value, one row per patient
# ---------------------------------------------------------------------------
w_patient = Window.partitionBy("patient_key").orderBy("start_ts")

visits = (
    gold_fact.filter(F.col("is_completed"))
    .withColumn("visit_seq", F.row_number().over(w_patient))
    .withColumn("prev_visit", F.lag("start_ts").over(w_patient))
    .withColumn("days_since_prev",
                F.datediff(F.to_date("start_ts"), F.to_date("prev_visit")))
)

mart_patient_summary = (
    visits.groupBy("patient_key", "src_patient_id")
    .agg(
        F.count("*").alias("completed_visits"),
        F.min("start_ts").alias("first_visit_ts"),
        F.max("start_ts").alias("last_visit_ts"),
        F.round(F.sum("revenue_synth"), 2).alias("lifetime_revenue"),
        F.round(F.avg("days_since_prev"), 1).alias("avg_days_between_visits"),
        F.countDistinct("clinic_name").alias("clinics_visited"),
        F.countDistinct("discipline").alias("disciplines_seen"),
    )
    .withColumn("days_since_last_visit",
                F.datediff(F.current_date(), F.to_date("last_visit_ts")))
    # A crude lapse definition, and it MUST be agreed with the business rather
    # than assumed. 90 days is a placeholder, not a clinical judgement.
    .withColumn("is_lapsed", F.col("days_since_last_visit") > 90)
    # Bands are PERCENTILE-based, not fixed thresholds. Fixed cutoffs need
    # recalibrating every time volume changes — at 4,000 appointments everyone
    # landed in Low, at 30,000 everyone landed in High. Percentiles separate
    # the cohort regardless of scale, which is also what a clinic actually
    # wants: "who are my top 20%", not "who crossed an arbitrary dollar line".
    .withColumn(
        "revenue_pctile",
        F.percent_rank().over(Window.orderBy("lifetime_revenue")),
    )
    .withColumn(
        "value_band",
        F.when(F.col("revenue_pctile") >= 0.80, "High")
        .when(F.col("revenue_pctile") >= 0.40, "Medium")
        .otherwise("Low"),
    )
    .orderBy(F.col("lifetime_revenue").desc())
)

mart_patient_summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{GOLD}.mart_patient_summary"
)
print(f"mart_patient_summary: {mart_patient_summary.count()} patients")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Summary — what the app can now query
# ---------------------------------------------------------------------------
for t in ["dim_date", "dim_clinic", "dim_practitioner", "dim_service",
          "fct_appointment", "fct_capacity",
          "mart_clinic_monthly", "mart_practitioner_monthly",
          "mart_service_monthly", "mart_patient_summary"]:
    n = spark.table(f"{CATALOG}.{GOLD}.{t}").count()
    print(f"  {t:<28} {n:>8,} rows")

display(
    spark.sql(f"""
        SELECT month_label, clinic_name, appointments_total, revenue, margin,
               dna_rate, utilisation_pct
        FROM   {CATALOG}.{GOLD}.mart_clinic_monthly
        ORDER  BY month_start DESC, revenue DESC
        LIMIT  20
    """)
)
