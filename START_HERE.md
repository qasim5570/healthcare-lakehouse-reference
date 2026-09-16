# START HERE

One edit, then four commands. Read this before touching anything else.

---

## What this repo is

A working vertical slice: Nookal appointments from extraction through to a
quality gate. Not five half-built sources — one that runs end to end, so the
second source is a pattern to clone rather than a new project.

## The one edit you must make

Open `databricks.yml`, find the `dev` target, replace the host:

```yaml
  dev:
    variables:
      catalog: avanti_dev                             # already set for you
    workspace:
      host: https://CHANGE-ME.azuredatabricks.net     # <-- YOUR WORKSPACE URL
```

Get the URL from your browser address bar with Databricks open. Everything up
to and including `.azuredatabricks.net`, nothing after it.

Leave the `prod` target alone. It stays obviously broken on purpose — nobody
can accidentally deploy to a placeholder.

## Prerequisites (already done if you followed the setup)

- Catalog `avanti_dev` exists, bound to your own ADLS container
- Schemas: `landing`, `bronze`, `silver`, `gold`, `ops`
- Volume `avanti_dev.landing.raw`

If not, run `scripts/setup_catalog.sql` first, replacing `avanti_dev`.

## Four commands

```bash
# 1. authenticate — opens a browser for SSO, writes a profile
databricks auth login --host https://YOUR-WORKSPACE.azuredatabricks.net

# 2. check the config parses and every reference resolves. Creates nothing.
databricks bundle validate -t dev

# 3. upload code + create the jobs. Still runs nothing.
databricks bundle deploy -t dev

# 4. trigger them
databricks bundle run ingest_nookal   -t dev
databricks bundle run medallion_build -t dev
```

Deploy and run are separate on purpose. Deploying is safe and repeatable;
running costs compute and moves data.

## What you should see

After `deploy`, the Jobs UI shows two jobs prefixed `[dev yourname]` with
**paused** schedules. That prefix and the pause come from `mode: development`
— nothing fires on a timer while you experiment, and two engineers deploying
to dev do not collide.

After `run`, in a SQL editor:

```sql
SELECT status, count(*) FROM avanti_dev.silver.fct_appointment GROUP BY status;
SELECT * FROM avanti_dev.ops.dq_results ORDER BY run_ts DESC;
```

`unknown_status_rate` will be non-zero. That is correct — the sample generator
deliberately emits an unmapped `wibble` status so you can see the check fire.

## Expected problems, and what they mean

**Job fails asking for compute.** `resources/jobs.yml` assumes serverless job
compute. Trial tier may not provide it. Fix is a `job_cluster` block — ask.

**`ModuleNotFoundError: avanti`.** The job files add the parent directory to
`sys.path` to import `avanti.transforms`. That depends on the working directory
Databricks gives a notebook task. Ask and we will correct the path.

**Volume not found.** `scripts/setup_catalog.sql` has not been run, or was run
against a different catalog.

## Reading order, if you want to understand rather than run

1. `databricks.yml` — environments and variables
2. `resources/jobs.yml` — the jobs themselves, and where `${var.catalog}` lands
3. `src/jobs/ingest_nookal.py` — the only plain Python; writes files
4. `src/jobs/bronze_load.py` — Auto Loader; files become a table
5. `src/jobs/silver_build.py` — Spark; table to table
6. `src/avanti/transforms.py` — pure logic, no Spark, unit tested
7. `tests/unit/test_transforms.py` — 39 tests, under a second, no cluster

The split at 6 and 7 is the important one: the functions under test are the
same functions running in production, not a copy that drifts.
