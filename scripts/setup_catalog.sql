-- ===========================================================================
-- One-time environment setup. Run BEFORE the first bundle deploy.
--
-- HOW TO RUN
--   clinic_dev is a placeholder, NOT Databricks SQL syntax. Either:
--     a) find-and-replace clinic_dev with your catalog name, or
--     b) run it from a notebook:
--
--          CATALOG = "clinic_dev"
--          sql = open("/Workspace/.../setup_catalog.sql").read()
--          for stmt in sql.split(";"):
--              if stmt.strip() and not stmt.strip().startswith("--"):
--                  spark.sql(stmt.replace("clinic_dev", CATALOG))
--
--   Run once per environment: clinic_dev, clinic_test, clinic_prod.
--
-- PREREQUISITE
--   The catalog itself must exist first, bound to YOUR storage:
--
--     CREATE CATALOG IF NOT EXISTS clinic_dev
--     MANAGED LOCATION 'abfss://lakehouse@<account>.dfs.core.windows.net/';
--
--   Without MANAGED LOCATION the catalog silently falls back to the
--   metastore default, which is Databricks-managed storage.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- SCHEMAS
--
-- Namespaces, not storage: creating one allocates nothing. What they give you
-- is a GRANT BOUNDARY, which is why analysts can be given gold without ever
-- seeing bronze — unconformed data produces confidently wrong answers.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS clinic_dev.landing
  COMMENT 'Raw files as landed from source APIs. Volumes only, no tables.';

CREATE SCHEMA IF NOT EXISTS clinic_dev.bronze
  COMMENT 'Append-only, as-landed. Never edited. The replay tape.';

CREATE SCHEMA IF NOT EXISTS clinic_dev.silver
  COMMENT 'Cleaned, deduplicated, conformed. Engineering only.';

CREATE SCHEMA IF NOT EXISTS clinic_dev.gold
  COMMENT 'Business-facing marts and metric views. The only layer analysts query.';

CREATE SCHEMA IF NOT EXISTS clinic_dev.gold_secure
  COMMENT 'PHI-bearing views. Restricted grants, deliberately separate from gold.';

CREATE SCHEMA IF NOT EXISTS clinic_dev.ops
  COMMENT 'Watermarks, ingest audit, data quality results, reconciliation, checkpoints.';

CREATE SCHEMA IF NOT EXISTS clinic_dev.ml
  COMMENT 'Feature tables, registered models, batch inference outputs.';


-- ---------------------------------------------------------------------------
-- VOLUMES — governed folders for FILES, as opposed to tables.
--
-- /Volumes/<catalog>/<schema>/<volume>/ then ordinary subdirectories.
-- Unity Catalog resolves that path to your cloud bucket and brokers a
-- short-lived, path-scoped credential per access. Nobody handles a storage key.
-- ---------------------------------------------------------------------------
CREATE VOLUME IF NOT EXISTS clinic_dev.landing.raw
  COMMENT 'Raw API responses, exactly as returned. The replay tape. Lifecycle: 90 days hot, then archive, then delete per retention policy.';

-- CRITICAL: checkpoints live in ops, NOT in landing.
--
-- The landing volume gets a lifecycle rule that deletes files after 90 days.
-- If Auto Loader checkpoints sat inside it, that rule would eventually delete
-- them — Auto Loader would forget which files it had consumed and REPROCESS
-- THE ENTIRE LANDING VOLUME, duplicating all of Bronze. You would discover it
-- months later as a mysterious doubling of row counts.
CREATE VOLUME IF NOT EXISTS clinic_dev.ops.checkpoints
  COMMENT 'Auto Loader checkpoints (RocksDB file tracking + inferred schemas). Operational state. NEVER subject to a lifecycle rule.';


-- ---------------------------------------------------------------------------
-- OPS TABLES
--
-- Operational metadata about the pipeline itself, not business data.
-- Separate schema because: different grants, different lifecycle (append-only
-- audit vs rebuildable business tables), and a Bronze full refresh must never
-- take the audit trail with it.
-- ---------------------------------------------------------------------------

-- Where incremental extraction got to, per source and entity.
-- Read at the start of a run, minus an overlap window. Committed ONLY after a
-- fully successful pull — committing on partial success creates a permanent
-- gap that reports as a successful run.
CREATE TABLE IF NOT EXISTS clinic_dev.ops.watermark (
  source              STRING  NOT NULL  COMMENT 'nookal | xero | hapana | alayacare | ghl',
  entity              STRING  NOT NULL  COMMENT 'appointments | patients | journals | ...',
  high_water_mark     TIMESTAMP         COMMENT 'Max source modification timestamp successfully extracted',
  last_batch_id       STRING            COMMENT 'The job run that last advanced this',
  updated_at          TIMESTAMP
) COMMENT 'Incremental extraction position. One row per source-entity pair.';

-- One row per extraction run. Written as RUNNING before any work, closed as
-- SUCCEEDED or FAILED afterwards. A row left RUNNING with a null finished_at
-- means the run died mid-flight — which is exactly the state you want visible.
--
-- This is what you show the business when asked "is this number stale?", and
-- what catches a job that has been succeeding while pulling zero rows.
CREATE TABLE IF NOT EXISTS clinic_dev.ops.ingest_audit (
  source              STRING,
  entity              STRING,
  batch_id            STRING    COMMENT 'The Databricks job run id, or manual-<uuid>',
  record_count        BIGINT,
  page_count          INT,
  status              STRING    COMMENT 'RUNNING | SUCCEEDED | FAILED',
  error_message       STRING,
  started_at          TIMESTAMP,
  finished_at         TIMESTAMP
) COMMENT 'One row per extraction run. A SUCCEEDED row with record_count = 0 should make you suspicious, not relieved.';

-- Quality gate output, appended every run. The value is in TRENDING these, not
-- just alerting: a phone-normalisation failure rate creeping from 8% to 20%
-- over a month is a real problem no single run would flag.
CREATE TABLE IF NOT EXISTS clinic_dev.ops.dq_results (
  check_name          STRING,
  severity            STRING    COMMENT 'fail (stops the pipeline) | warn (recorded only)',
  value               DOUBLE,
  threshold           DOUBLE,
  passed              BOOLEAN,
  run_ts              TIMESTAMP
) COMMENT 'Quality gate results. Trend them, do not just alert on them.';

-- Reconciliation variances. Expectations catch MALFORMED data; reconciliation
-- catches WRONG data, which is what destroys credibility with a finance team.
CREATE TABLE IF NOT EXISTS clinic_dev.ops.recon_results (
  check_name          STRING    COMMENT 'gl_tie_out | appointment_count | sah_three_way | ...',
  grain               STRING    COMMENT 'What the variance is measured at, e.g. account_code + period',
  grain_value         STRING,
  source_value        DOUBLE    COMMENT 'What the source system says',
  platform_value      DOUBLE    COMMENT 'What our tables say',
  variance            DOUBLE,
  tolerance           DOUBLE,
  passed              BOOLEAN,
  run_ts              TIMESTAMP
) COMMENT 'Cross-system reconciliation. GL tie-out tolerance is zero.';


-- ---------------------------------------------------------------------------
-- VERIFY — run these after the above and check the output.
-- ---------------------------------------------------------------------------
-- SHOW SCHEMAS IN clinic_dev;
--   expect: landing, bronze, silver, gold, gold_secure, ops, ml
--
-- SHOW VOLUMES IN clinic_dev.landing;    -- expect: raw
-- SHOW VOLUMES IN clinic_dev.ops;        -- expect: checkpoints
--
-- SHOW TABLES IN clinic_dev.ops;
--   expect: watermark, ingest_audit, dq_results, recon_results
--
-- DESCRIBE CATALOG EXTENDED clinic_dev;
--   confirm the storage location is YOUR abfss:// path, not a
--   Databricks-managed default


-- ---------------------------------------------------------------------------
-- GRANTS — commented out for a personal sandbox where you own everything.
-- Uncomment for the client once the groups exist.
--
-- Grant at SCHEMA level, to GROUPS, never to individuals and never
-- table-by-table. That is what keeps the model reviewable and the quarterly
-- access review tractable.
--
-- Note the deliberate asymmetry: analysts get gold ONLY. Not for secrecy —
-- because querying Bronze directly would count cancelled appointments as
-- completed, since Bronze holds raw source statuses before canonical_status
-- has run.
-- ---------------------------------------------------------------------------
-- GRANT USE CATALOG ON CATALOG clinic_dev TO `clinic_analysts`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA clinic_dev.gold TO `clinic_analysts`;
--
-- GRANT USE CATALOG ON CATALOG clinic_dev TO `clinic_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.landing TO `clinic_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.bronze  TO `clinic_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.silver  TO `clinic_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.gold    TO `clinic_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.ops     TO `clinic_engineers`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.ml      TO `clinic_engineers`;
--
-- Identifiable detail lives here, with a much shorter grant list.
-- GRANT USE SCHEMA, SELECT ON SCHEMA clinic_dev.gold_secure TO `clinic_clinical_leads`;
--
-- SERVICE PRINCIPALS — production jobs do NOT run as a human, so every
-- privilege the pipeline relies on must be granted explicitly. Miss these and
-- the scheduled job fails at 05:00 with a permission error.
-- GRANT USE CATALOG ON CATALOG clinic_dev TO `sp-clinic-ingest`;
-- GRANT USE SCHEMA, WRITE VOLUME ON SCHEMA clinic_dev.landing TO `sp-clinic-ingest`;
-- GRANT USE SCHEMA, WRITE VOLUME, SELECT, MODIFY ON SCHEMA clinic_dev.ops TO `sp-clinic-ingest`;
--
-- GRANT USE CATALOG ON CATALOG clinic_dev TO `sp-clinic-prod`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.bronze TO `sp-clinic-prod`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.silver TO `sp-clinic-prod`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.gold   TO `sp-clinic-prod`;
-- GRANT ALL PRIVILEGES ON SCHEMA clinic_dev.ops    TO `sp-clinic-prod`;
