"""Bronze layer: land the raw daily CSV as-is, plus ingestion metadata.

Bronze is an immutable copy of whatever the source delivered. Nothing is
cleaned here on purpose - if a downstream transformation turns out to be
wrong, silver and gold can always be rebuilt from bronze without going back
to the source bucket.
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def locate_daily_file(cfg: dict, run_date: str) -> Path:
    """Find the raw CSV for the given run date (YYYYMMDD).

    Locally this is a filesystem lookup. In production the same contract is
    covered by an S3 key check - the Airflow DAG uses a sensor, so this job
    only starts once the object actually exists.
    """
    file_name = cfg["source"]["file_pattern"].format(date=run_date)
    path = Path(cfg["paths"]["raw"]) / file_name
    if not path.exists():
        raise FileNotFoundError(f"expected daily file not found: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"daily file is empty: {path}")
    return path


def ingest_to_bronze(spark: SparkSession, cfg: dict, run_date: str) -> DataFrame:
    """Read the raw CSV and append it to the bronze layer, partitioned by day."""
    source_path = locate_daily_file(cfg, run_date)

    # Read everything as string: bronze has to accept whatever arrives, even
    # rows that would fail a typed read. Casting happens on the way to silver.
    df = (
        spark.read.option("header", True)
        .csv(str(source_path))
        .withColumn("_source_file", F.lit(source_path.name))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_date", F.lit(run_date))
    )

    bronze_path = f"{cfg['paths']['bronze']}/network_metrics"
    (
        df.write.mode("overwrite")
        .partitionBy("_run_date")
        .option("partitionOverwriteMode", "dynamic")
        .parquet(bronze_path)
    )
    logger.info("bronze ingest done: %s rows from %s", df.count(), source_path.name)
    return df
