"""Silver layer: validated, typed, deduplicated records with usable timestamps.

Reads the day's bronze partition, runs the data quality gate, converts the
unix timestamp to an event timestamp (UTC) and derives the date/hour columns
that the gold aggregations group on.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import validation

logger = logging.getLogger(__name__)


def build_silver(spark: SparkSession, cfg: dict, run_date: str) -> DataFrame:
    bronze_path = f"{cfg['paths']['bronze']}/network_metrics"
    df = spark.read.parquet(bronze_path).filter(F.col("_run_date") == run_date)

    vlog = validation.ValidationLog(cfg, run_date)
    source_file = df.select("_source_file").first()["_source_file"]
    validation.validate_file(source_file, df.columns, cfg, vlog)

    valid, rejected = validation.apply_row_checks(df, cfg, vlog)
    validation.write_quarantine(rejected, cfg, run_date)
    valid = validation.apply_dataset_checks(valid, cfg, vlog)

    silver = (
        valid.select(
            F.col("tower_id_t").alias("tower_id"),
            F.trim(F.col("region")).alias("region"),
            F.col("timestamp_t").alias("unix_timestamp"),
            F.col("signal_strength_t").alias("signal_strength"),
            F.col("data_volume_mb_t").alias("data_volume_mb"),
            F.col("_volume_anomaly").alias("is_volume_anomaly"),
            "_source_file",
            "_ingested_at",
        )
        .withColumn("event_ts", F.to_timestamp(F.from_unixtime("unix_timestamp")))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .dropDuplicates(["tower_id", "region", "unix_timestamp"])
    )

    silver_path = f"{cfg['paths']['silver']}/network_metrics"
    (
        silver.write.mode("overwrite")
        .partitionBy("event_date")
        .option("partitionOverwriteMode", "dynamic")
        .parquet(silver_path)
    )
    logger.info("silver build done: %s rows for %s", silver.count(), run_date)
    return silver
