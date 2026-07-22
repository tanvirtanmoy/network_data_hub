-- Gold: aggregates consumed by the operations team (and Superset).

-- grain: (event_date, event_hour, region)
CREATE TABLE IF NOT EXISTS lakehouse.gold.region_hourly_metrics (
    event_date           DATE,
    event_hour           INT,
    region               STRING,
    total_data_volume_mb DOUBLE,
    avg_signal_strength  DOUBLE,
    measurement_count    BIGINT COMMENT 'rows aggregated into this cell',
    active_towers        BIGINT COMMENT 'distinct towers reporting that hour'
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('comment' = 'hourly network load and quality per region');

-- grain: (event_date, region) - the daily report
CREATE TABLE IF NOT EXISTS lakehouse.gold.region_daily_summary (
    event_date             DATE,
    region                 STRING,
    total_data_volume_mb   DOUBLE,
    avg_signal_strength    DOUBLE,
    measurement_count      BIGINT,
    active_towers          BIGINT,
    anomalous_measurements BIGINT COMMENT 'rows flagged by the volume z-score check'
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('comment' = 'daily per-region summary for the ops report');


-- example ops queries -------------------------------------------------------

-- busiest region over the last 7 days
-- SELECT region, SUM(total_data_volume_mb) AS volume_7d
-- FROM lakehouse.gold.region_daily_summary
-- WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS
-- GROUP BY region
-- ORDER BY volume_7d DESC;

-- hours with degraded signal (worth a look by the RF team)
-- SELECT event_date, event_hour, region, avg_signal_strength
-- FROM lakehouse.gold.region_hourly_metrics
-- WHERE avg_signal_strength < -85
-- ORDER BY avg_signal_strength ASC;
