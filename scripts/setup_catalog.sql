-- One-time setup. Run this in a SQL editor or notebook BEFORE the first
-- bundle deploy. Replace ${catalog} with your target catalog.
--
-- On a personal workspace the default catalog is usually `workspace`, so you
-- can run this as-is with :catalog = workspace. For Avanti it becomes
-- avanti_dev / avanti_test / avanti_prod, created once each.

-- ---------------------------------------------------------------------------
-- Schemas. These are namespaces, not storage: creating one allocates nothing.
-- What they give you is a grant boundary, which is why analysts can be given
-- gold without ever seeing bronze.
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ${catalog}.landing
  COMMENT 'Raw files as landed from source APIs. Volumes only, no tables.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
  COMMENT 'Append-only, as-landed. Never edited. The replay tape.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
  COMMENT 'Cleaned, deduplicated, conformed. Engineering only.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
  COMMENT 'Business-facing marts and metric views. The only layer analysts query.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.ops
  COMMENT 'Watermarks, ingest audit, data quality results, reconciliation.';

-- ---------------------------------------------------------------------------
-- Landing volume. This is the only place raw files live. The /Volumes path is
-- Unity Catalog's governed alias for a folder in your cloud bucket.
-- ---------------------------------------------------------------------------
CREATE VOLUME IF NOT EXISTS ${catalog}.landing.raw
  COMMENT 'Raw API responses. Lifecycle: 90 days hot, then archive.';

-- ---------------------------------------------------------------------------
-- Ops tables.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.ops.watermark (
  source              STRING  NOT NULL,
  entity              STRING  NOT NULL,
  high_water_mark     TIMESTAMP,
  last_batch_id       STRING,
  updated_at          TIMESTAMP
) COMMENT 'Incremental extraction position. Committed only after a successful full pull.';

CREATE TABLE IF NOT EXISTS ${catalog}.ops.ingest_audit (
  source              STRING,
  entity              STRING,
  batch_id            STRING,
  record_count        BIGINT,
  page_count          INT,
  status              STRING,
  error_message       STRING,
  started_at          TIMESTAMP,
  finished_at         TIMESTAMP
) COMMENT 'One row per extraction run. This is what you show the business when asked if a number is stale.';

CREATE TABLE IF NOT EXISTS ${catalog}.ops.dq_results (
  check_name          STRING,
  severity            STRING,
  value               DOUBLE,
  threshold           DOUBLE,
  passed              BOOLEAN,
  run_ts              TIMESTAMP
) COMMENT 'Quality gate output, appended every run. Trend these, do not just alert on them.';

-- ---------------------------------------------------------------------------
-- Grants. Commented out for a personal workspace; uncomment for Avanti once
-- the groups exist. Note these grant at SCHEMA level, to GROUPS, never to
-- individuals - that is what keeps the model reviewable.
-- ---------------------------------------------------------------------------
-- GRANT USE CATALOG ON CATALOG ${catalog} TO avanti_analysts;
-- GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.gold TO avanti_analysts;
-- GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.bronze TO avanti_engineers;
-- GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.silver TO avanti_engineers;
-- GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.gold   TO avanti_engineers;
