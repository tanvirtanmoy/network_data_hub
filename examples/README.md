# Example run: a day with bad data

`data/raw/network_metrics_20250724.csv` is a second sample day I generated with
deliberate problems mixed into otherwise normal rows, to show how the DQ gate
behaves. The files here are the actual outputs of running
`python scripts/run_local_pipeline.py --date 20250724`.

## What's in here

| file | what it shows |
|---|---|
| `validation_20250724.jsonl` | the structured event log the validation step writes - one JSON line per check, with PASS / WARN / REJECTED / FAIL status |
| `quarantined_rows_20250724.csv` | the 7 rejected rows, each with a `_dq_reason` column saying exactly why it was kicked out |
| `region_daily_summary.csv` | the daily gold report, built from the rows that survived |
| `region_hourly_metrics.csv` | the hourly gold table for the same day |

## How failures are handled

- **Bad rows are quarantined, not dropped.** Every rejected row is written to
  `data/quarantine/` with its reason, so nothing disappears silently and rows
  can be replayed after a fix upstream.
- **Warnings don't block.** Duplicates, towers reporting from several regions
  and volume outliers (the 5000 MB row got flagged by the z-score check) are
  logged as WARN and, where useful, carried into silver as a flag column
  (`is_volume_anomaly`) - the ops team sees them in the daily summary as
  `anomalous_measurements`.
- **The run only stops hard when it should.** A wrong file name, a missing
  column, or more than 10% of rows rejected raises `DataQualityError`, which
  fails the Airflow task and triggers the incident notification. 7 bad rows
  out of 82 (~8.5%) stays under that threshold, so this run completed and
  the report still went out.
