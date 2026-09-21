# Healthcare Lakehouse Reference

Medallion lakehouse on Databricks. Sources land as raw files, Bronze keeps them
immutable, Silver conforms them, Gold serves the business.

This repo is deliberately a **skeleton with one working vertical slice** —
Nookal appointments, end to end — rather than a half-built version of all five
sources. Get this running, then clone the pattern.

---

## What is here

```
databricks.yml              bundle root: targets, variables
resources/jobs.yml          job + schedule definitions
src/lakehouse/transforms.py    pure logic (no Spark) - the unit-tested core
src/jobs/ingest_nookal.py   extract -> landing volume  (plain Python)
src/jobs/bronze_load.py     landing -> bronze          (Auto Loader)
src/jobs/silver_build.py    bronze -> silver           (PySpark)
src/jobs/quality_gate.py    assertions that block publication
tests/unit/                 pytest, runs in <1s, no cluster
scripts/setup_catalog.sql   one-time schema + volume creation
.github/workflows/ci.yml    test on PR, deploy on merge
```

The split that matters: **`src/lakehouse/` is Spark-free and unit-tested;
`src/jobs/` is where Spark runs.** The jobs import the pure functions rather
than reimplementing them, so the logic under test is the logic in production.

---

## First deploy, on your own workspace

### 1. Install the CLI and authenticate

```bash
brew tap databricks/tap && brew install databricks    # macOS
databricks --version                                   # expect v0.2xx+

databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

### 2. Point the bundle at your workspace

Edit `databricks.yml` → `targets.dev.workspace.host`. If your catalog is not
called `workspace`, change `targets.dev.variables.catalog` too. Check with:

```sql
SELECT current_catalog();
```

### 3. Create the schemas and volume

Open `scripts/setup_catalog.sql` in a SQL editor, replace `clinic_dev` with
your catalog name, and run it. This is one-time.

### 4. Validate and deploy

```bash
databricks bundle validate -t dev     # YAML syntax and references
databricks bundle deploy   -t dev     # creates the jobs in your workspace
databricks bundle summary  -t dev     # what got deployed
```

Because `dev` uses `mode: development`, every object is prefixed
`[dev <yourname>]` and **all schedules are paused**. Nothing fires on a timer.

### 5. Run the chain

```bash
databricks bundle run ingest_nookal   -t dev   # lands ~600 sample records
databricks bundle run medallion_build -t dev   # bronze -> silver -> gate
```

Then confirm in a SQL editor:

```sql
SELECT status, count(*) FROM workspace.silver.fct_appointment GROUP BY status;
SELECT * FROM workspace.ops.dq_results ORDER BY run_ts DESC;
```

### 6. Run the tests locally

```bash
pip install -e ".[dev]"
pytest tests/unit -v
```

39 tests, well under a second, no cluster required.

---

## The sample data is deliberately messy

`ingest_nookal.py` with `mode=sample` generates phone numbers in five formats,
mixed-case emails, missing fields, and an unmapped `wibble` status. Clean sample
data lets bugs through that real data would catch — the whole point of the
pipeline is surviving mess, so the fixture has to contain some.

Watch `unknown_status_rate` in the quality gate output. It should be non-zero,
because `wibble` is intentionally unmapped. That is the check working.

---

## Wiring up a real source

When Nookal credentials arrive:

1. Create a secret scope and store the key:
   ```bash
   databricks secrets create-scope clinic
   databricks secrets put-secret clinic nookal_api_key
   ```
2. Implement `fetch_api_page()` in `src/jobs/ingest_nookal.py`. The docstring
   lists the four non-negotiables (watermark discipline, overlap window,
   backoff, audit logging).
3. Flip the job parameter `mode` from `sample` to `api` in `resources/jobs.yml`.

Nothing downstream changes. Bronze, Silver and the gate do not know or care
where the JSON came from — which is exactly why the sample mode is useful for
more than a demo.

Adding a second source is: a new `ingest_<source>.py`, a new job block, and a
new branch in `silver_build.py`. The extractor framework is worth factoring out
once you have three.

---

## Promoting to production

When their workspace exists:

1. Fill in `targets.prod.workspace.host`.
2. Create the service principal `sp-clinic-prod` and grant it on the catalog.
3. Create `clinic_dev` / `clinic_test` / `clinic_prod` catalogs and run
   `setup_catalog.sql` against each.
4. Add repo secrets in GitHub: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`,
   `DATABRICKS_CLIENT_SECRET` (from `sp-clinic-cicd`).
5. `databricks bundle deploy -t prod`.

`mode: production` refuses to deploy unless `run_as` is a service principal,
and it does not prefix or pause anything. That asymmetry with dev is the point.

---

## Daily loop

```bash
git checkout -b feat/hapana-extractor
# ...edit...
pytest tests/unit                       # fast feedback, no cluster
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run ingest_hapana -t dev
git add -A && git commit -m "Add Hapana member extractor"
git push -u origin feat/hapana-extractor
gh pr create --fill
```

CI runs on the PR. Merge to `main` deploys to prod.

---

## Conventions

| Thing | Rule |
|---|---|
| Bronze table | `bronze.{source}_{entity}` — mirrors the source, no modelling |
| Silver fact | `silver.fct_{business_process}` |
| Silver dim | `silver.dim_{grain}` |
| Gold mart | `gold.mart_{domain}__{subject}` |
| Surrogate key | `{grain}_key`, deterministic SHA-256. Never a sequence. |
| Timestamps | `*_ts` is UTC. `*_date` is a local calendar date via `dim_date`. |
| Deletions | Soft-delete via `is_deleted`. Never physically delete from Bronze. |

---

## Things this skeleton does not do yet

Named honestly so they do not get forgotten:

- **Watermarks are defined but unused** — sample mode pulls everything each run.
- **No deletion sweep.** Records deleted at source stay in Silver forever until
  this is built. It is the gap that produces wrong numbers most quietly.
- **No `party_xref`.** `party_candidate` is populated; connected-component
  assignment across sources comes when the second source lands.
- **No Gold layer.** Deliberate: Gold shapes follow from the marts in the
  architecture doc, and building it against one source would bake in the wrong
  grain.
- **Silver is `overwrite`, not incremental.** Fine at this volume, wrong at
  three years of history. Move to `MERGE` or declarative CDC before Phase 2.
