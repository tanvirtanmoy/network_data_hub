"""Gold layer: the aggregates the operations team actually consumes.

Two tables come out of silver:

- region_hourly_metrics  grain (event_date, event_hour, region)
- region_daily_summary   grain (event_date, region) - the daily report

Both are written as Parquet (Iceberg tables in production) and also as plain
CSV files under gold/reports/<date>/ so the daily report can be mailed around
or loaded into Superset without any extra tooling.
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def _write(df: DataFrame, base: str, name: str, run_date: str) -> None:
    (
        df.write.mode("overwrite")
        .partitionBy("event_date")
        .option("partitionOverwriteMode", "dynamic")
        .parquet(f"{base}/{name}")
    )
    # small human-readable copy for the ops team / Superset
    csv_dir = Path(base) / "reports" / run_date
    csv_dir.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(csv_dir / f"{name}.csv", index=False)


def build_gold(spark: SparkSession, cfg: dict, run_date: str) -> dict[str, DataFrame]:
    date_iso = f"{run_date[:4]}-{run_date[4:6]}-{run_date[6:]}"
    silver = (
        spark.read.parquet(f"{cfg['paths']['silver']}/network_metrics")
        .filter(F.col("event_date") == date_iso)
    )

    hourly = (
        silver.groupBy("event_date", "event_hour", "region")
        .agg(
            F.round(F.sum("data_volume_mb"), 2).alias("total_data_volume_mb"),
            F.round(F.avg("signal_strength"), 2).alias("avg_signal_strength"),
            F.count("*").alias("measurement_count"),
            F.countDistinct("tower_id").alias("active_towers"),
        )
        .orderBy("event_date", "event_hour", "region")
    )

    daily = (
        silver.groupBy("event_date", "region")
        .agg(
            F.round(F.sum("data_volume_mb"), 2).alias("total_data_volume_mb"),
            F.round(F.avg("signal_strength"), 2).alias("avg_signal_strength"),
            F.count("*").alias("measurement_count"),
            F.countDistinct("tower_id").alias("active_towers"),
            F.sum(F.col("is_volume_anomaly").cast("int")).alias("anomalous_measurements"),
        )
        .orderBy("event_date", "region")
    )

    gold_base = cfg["paths"]["gold"]
    _write(hourly, gold_base, "region_hourly_metrics", run_date)
    _write(daily, gold_base, "region_daily_summary", run_date)
    logger.info(
        "gold build done for %s: %s hourly rows, %s daily rows",
        run_date, hourly.count(), daily.count(),
    )
    return {"hourly": hourly, "daily": daily}
