# Network Data Hub

Data ingestion pipeline for daily telecom network metrics.

Cell towers upload a raw CSV (`network_metrics_YYYYMMDD.csv`) to an S3 bucket every day.
This project ingests those files and processes them through a medallion architecture
(bronze → silver → gold) with data quality checks, producing hourly and daily
per-region aggregates for the operations team.

Everything runs locally end-to-end (the filesystem stands in for S3, Parquet for
Iceberg), and the same job code is what the Airflow DAG in `dags/` orchestrates
in production. `examples/` contains the outputs of a run over a deliberately
dirty sample day, including the validation log and the quarantined rows.

## Stack

- **Orchestration:** Apache Airflow
- **Processing:** PySpark
- **Storage:** S3 + Iceberg (simulated locally with the filesystem + Parquet)

## Architecture

```
 s3://raw-telecom-network-data          (daily upload from towers)
        │
        ▼
 [sensor: wait for today's file]        S3KeySensor in prod, 4h timeout
        │
        ▼
 BRONZE  raw copy, all strings, + lineage columns     partitioned by _run_date
        │
        ▼
 [data quality gate]                    reject → quarantine/ (with reason)
        │                               events → logs/validation_<date>.jsonl
        ▼
 SILVER  typed, deduped, event_ts/date/hour derived   partitioned by event_date
        │
        ▼
 GOLD    region_hourly_metrics                        partitioned by event_date
         region_daily_summary
        │
        ▼
 csv report per day (gold/reports/) / Superset
```

| layer | what it holds | why |
|---|---|---|
| bronze | the file exactly as delivered, every column as string, plus `_source_file`, `_ingested_at`, `_run_date` | must accept anything the source sends; if a transformation turns out wrong later, silver and gold can be rebuilt from here without touching the source bucket |
| silver | validated, typed, deduplicated measurements with a proper UTC timestamp | one clean, trustworthy table other teams could also build on |
| gold | small aggregates at exactly the grain the ops team asks about | cheap to query, safe to point a dashboard at |

## Data models

Schemas for all three layers are in `sql/` as Iceberg DDL, matching what the
jobs actually write.

- **Bronze** is partitioned by `_run_date` (one partition per upload day), which
  makes reruns idempotent: reprocessing a day dynamically overwrites just that
  partition.
- **Silver and gold** are partitioned by `event_date` — that's what every
  downstream query filters on, so partition pruning does the heavy lifting.
- Silver's dedup key is `(tower_id, region, unix_timestamp)`; the grain of the
  gold tables is `(date, hour, region)` and `(date, region)` respectively.
- In production these are Iceberg tables (schema evolution, time travel,
  snapshot rollback when a bad load slips through). Locally plain Parquet with
  the same layout is enough to prove the logic.

## Data quality

All checks run in one place: the gate between bronze and silver
(`src/validation.py`). Bronze stays untouched by design, and nothing reaches
silver without passing.

| check | type | on failure |
|---|---|---|
| file naming convention | file | stop the run |
| expected columns present | file | stop the run (extra columns only warn — schema drift) |
| type casts (`try_cast`), nulls | row | reject row → quarantine |
| signal strength in [-120, -30] dBm | row | reject row → quarantine |
| data volume ≥ 0 | row | reject row → quarantine |
| duplicates on (tower, region, ts) | dataset | warn, keep one copy |
| tower reporting from several regions | dataset | warn (see assumptions) |
| volume outliers, z-score > 3 per region | dataset | warn + `is_volume_anomaly` flag carried to silver and counted in the daily report |

Principles:

- **Quarantine, don't drop.** Rejected rows land in `data/quarantine/` with a
  `_dq_reason` column, so nothing disappears silently and rows can be replayed
  after an upstream fix.
- **Fail hard only when it matters.** A handful of bad rows shouldn't hold the
  daily report hostage — but if more than 10% of a file is rejected, something
  upstream is broken and the run stops with `DataQualityError`.
- **Every check emits a structured JSON event** to
  `logs/validation_<date>.jsonl` (see `examples/validation_20250724.jsonl`).
  Monitoring hangs off this stream rather than off log grepping.

## Monitoring & alerting

What's in the DAG already:

- **Retries:** every task retries 2x with a 5 minute delay before it counts as
  failed — transient S3/cluster hiccups don't page anyone.
- **Freshness:** the file sensor runs in `reschedule` mode (no worker slot held)
  and times out after 4 hours. A missing or late upload is an alert, not
  a silent no-op day.
- **Incident notification:** `on_failure_callback` posts the DAG id, task,
  run date and a link to the task log to a Slack channel via webhook, with
  `email_on_failure` as a fallback so alerting never depends on Slack alone.
  The callback is wrapped in try/except — the notifier itself can never take
  the DAG down.

What I'd add next in a real deployment:

- Ship the validation events to CloudWatch/Datadog and alarm on any `FAIL`
  event and on `WARN` rate spikes.
- Track `rejected_fraction` per day as a metric — a slow upward trend is an
  upstream data problem long before it crosses the 10% hard stop.
- An SLA on gold table publish time, since that's what the ops team actually
  consumes.

## Visualization (Superset)

To cover the optional Superset part I spun up a local instance with Docker,
loaded the gold tables into a small Postgres, and built the daily report chart
on top of `region_daily_summary`:

![daily data volume per region](docs/img/superset_daily_volume_per_region.png)

The 2025-07-24 South spike is the seeded 5000 MB anomaly from the dirty sample
day - satisfying to see it jump out visually after the DQ gate flagged it.

To reproduce:

```bash
docker network create superset-demo
docker run -d --name superset-demo-db --network superset-demo \
  -e POSTGRES_USER=superset -e POSTGRES_PASSWORD=superset -e POSTGRES_DB=gold \
  -p 5433:5432 postgres:16
# load the gold report CSVs into postgres (see data/gold/reports/), then:
docker run -d --name superset-demo --network superset-demo -p 8088:8088 \
  -e SUPERSET_SECRET_KEY=change-me apache/superset:4.1.1
docker exec superset-demo pip install psycopg2-binary && docker restart superset-demo
docker exec superset-demo superset db upgrade
docker exec superset-demo superset fab create-admin --username admin \
  --firstname a --lastname d --email admin@local --password admin
docker exec superset-demo superset init
```

Then add `postgresql+psycopg2://superset:superset@superset-demo-db:5432/gold`
as a database in the Superset UI and build charts on the two gold tables.

## Running it locally

Needs Python 3.10+ and a Java 17 runtime (Spark requirement).

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
export JAVA_HOME=/path/to/jdk-17

# clean sample day
.venv/bin/python scripts/run_local_pipeline.py --date 20250723

# sample day with seeded bad rows - watch the DQ gate work
.venv/bin/python scripts/run_local_pipeline.py --date 20250724

# tests
.venv/bin/python -m pytest tests/ -q
```

The runner prints the daily summary at the end; reports land in
`data/gold/reports/<date>/`.

## Repo layout

```
├── config/     # pipeline configuration (paths, DQ thresholds, spark)
├── dags/       # Airflow DAG (lives on the scheduler in prod)
├── src/        # PySpark jobs: ingestion, validation, silver, gold
├── scripts/    # local runner, mirrors the DAG task by task
├── sql/        # Iceberg DDL for the three layers + example queries
├── tests/      # pytest suite for the validation rules + dirty fixture
├── examples/   # real outputs of the dirty-day run (logs, quarantine, reports)
└── data/raw/   # simulated S3 landing bucket with the two sample days
```

## How I broke the work down

Roughly in the order of the commit history:

1. **Scaffold** — repo layout, config file, requirements, sample data in place.
2. **Foundations** — config loader and a shared Spark session factory, so every
   job reads the same settings and swapping local paths for S3 stays a
   config-only change.
3. **Bronze ingestion** plus a **local runner** early, so every later step
   could be executed end-to-end instead of reviewed on faith.
4. **The DQ gate** before silver — the checks shape what silver is allowed to
   contain, so they had to exist first.
5. **Silver, then gold** — typing/dedup/timestamps, then the two aggregate
   tables the ops team asked for.
6. **Airflow DAG** once the jobs were stable: sensor, three tasks, retries,
   failure notifications.
7. **Iceberg DDL** for the three layers, matching what the jobs actually write.
8. **A dirty sample day** to prove the DQ gate does what the code claims —
   this is also what surfaced the ANSI-mode cast bug (fixed with `try_cast`).
9. **Tests** for the validation rules, then this README.

## Assumptions

- **Towers report from multiple regions.** In the sample file the same
  `tower_id` shows up in several regions, which physically shouldn't happen for
  a fixed tower. I treated `region` as an attribute of the measurement rather
  than of the tower (aggregations follow the data), and the DQ gate flags the
  situation as a warning so someone can chase the feed. If a tower→region
  master table existed, I'd validate against it instead.
- Timestamps are epoch seconds in UTC; all derived times stay UTC.
- One file per day, and a rerun for the same date replaces that day's
  partitions (idempotent dynamic partition overwrite) rather than appending.
- The local filesystem stands in for S3 with the same path layout, so swapping
  the config paths for `s3a://` URIs is the only change needed.

## Possible improvements

- Real Iceberg tables with `MERGE INTO` for late-arriving corrections, plus
  compaction and snapshot retention jobs.
- Replace the hand-rolled checks with Great Expectations (or dbt tests if the
  silver/gold steps move to SQL) to get the docs/UI for free.
- A proper Superset dashboard (the setup above has single charts; a dashboard
  with a date filter would be the next step) and an hourly signal heatmap.
- CI running the pytest suite on every push.
- Backfill support is nearly free: enable `catchup=True` and the DAG reruns
  historical dates partition by partition.
- Cost: gold CSVs are tiny, but bronze/silver should get lifecycle rules
  (e.g. bronze to infrequent-access after 90 days) and the small daily files
  compacted periodically.
