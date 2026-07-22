-- Silver: validated, typed, deduplicated measurements.
-- Rows only get here after the DQ gate: schema check, type casts, null and
-- range checks. Rejected rows are quarantined with a reason instead of being
-- dropped. Dedup key is (tower_id, region, unix_timestamp).

CREATE TABLE IF NOT EXISTS lakehouse.silver.network_metrics (
    tower_id          INT       COMMENT 'cell tower id',
    region            STRING    COMMENT 'reporting region (trimmed)',
    unix_timestamp    BIGINT    COMMENT 'original epoch seconds from the feed',
    signal_strength   DOUBLE    COMMENT 'dBm, validated to [-120, -30]',
    data_volume_mb    DOUBLE    COMMENT 'transferred volume, validated >= 0',
    is_volume_anomaly BOOLEAN   COMMENT 'per-region z-score outlier flag (warn-only)',

    event_ts          TIMESTAMP COMMENT 'measurement time, UTC',
    event_date        DATE,
    event_hour        INT,

    _source_file      STRING,
    _ingested_at      TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'comment' = 'clean tower measurements, grain: one row per tower/region/timestamp',
    'write.format.default' = 'parquet'
);
