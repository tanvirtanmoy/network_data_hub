-- Bronze: immutable, as-delivered copy of the daily tower uploads.
-- Everything is kept as STRING on purpose: bronze must accept whatever the
-- source sends, including rows a typed read would reject. Typing and
-- cleaning happen on the way to silver, and silver/gold can always be
-- rebuilt from here without re-fetching from the source bucket.

CREATE TABLE IF NOT EXISTS lakehouse.bronze.network_metrics (
    tower_id        STRING,
    region          STRING,
    timestamp       STRING,
    signal_strength STRING,
    data_volume_mb  STRING,

    -- lineage metadata added at ingestion
    _source_file    STRING    COMMENT 'name of the raw csv this row came from',
    _ingested_at    TIMESTAMP COMMENT 'when the pipeline landed the row',
    _run_date       STRING    COMMENT 'pipeline run date, YYYYMMDD'
)
USING iceberg
PARTITIONED BY (_run_date)
TBLPROPERTIES (
    'comment' = 'raw daily tower metrics, one partition per upload day',
    'write.format.default' = 'parquet'
);
