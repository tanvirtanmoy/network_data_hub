"""Data quality checks, applied between bronze and silver.

Three groups of checks:

1. file-level    - naming convention, header matches the contract
2. row-level     - type casts, nulls, value ranges. Failing rows go to
                   quarantine with a reason, the rest continue.
3. dataset-level - statistical checks (duplicates, volume outliers, towers
                   reporting from several regions). Warn-only: they flag
                   things for the ops team but never block the pipeline.

Every check writes a structured JSON event to logs/validation_<date>.jsonl,
so monitoring can be built on the log stream (or the events shipped to
CloudWatch/Datadog as they are). The run only fails hard when the file itself
is unusable or the share of rejected rows crosses the configured threshold -
a handful of bad rows should not block the whole daily report.
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

logger = logging.getLogger(__name__)

FILE_NAME_RE = re.compile(r"^network_metrics_\d{8}\.csv$")


class DataQualityError(Exception):
    """A check failed hard enough that the run must stop."""


class ValidationLog:
    """Append-only JSON-lines log of validation events for one run."""

    def __init__(self, cfg: dict, run_date: str):
        log_dir = Path(cfg["paths"]["logs"])
        log_dir.mkdir(parents=True, exist_ok=True)
        self.path = log_dir / f"validation_{run_date}.jsonl"
        self.run_date = run_date

    def event(self, check: str, status: str, **details) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "run_date": self.run_date,
            "check": check,
            "status": status,  # PASS | WARN | REJECTED | FAIL
            **details,
        }
        with open(self.path, "a") as f:
            f.write(json.dumps(record) + "\n")
        log_fn = logger.warning if status in ("WARN", "REJECTED", "FAIL") else logger.info
        log_fn("[DQ] %s: %s %s", check, status, details)


def validate_file(file_name: str, columns: list[str], cfg: dict, vlog: ValidationLog) -> None:
    """File-level checks: naming convention and header contract."""
    if not FILE_NAME_RE.match(file_name):
        vlog.event("file_naming", "FAIL", file=file_name)
        raise DataQualityError(f"file name does not match convention: {file_name}")
    vlog.event("file_naming", "PASS", file=file_name)

    expected = cfg["source"]["expected_columns"]
    missing = [c for c in expected if c not in columns]
    extra = [c for c in columns if c not in expected and not c.startswith("_")]
    if missing:
        vlog.event("schema_columns", "FAIL", missing=missing, extra=extra)
        raise DataQualityError(f"schema check failed, missing columns: {missing}")
    if extra:
        # schema drift: tolerated but flagged so someone follows up
        vlog.event("schema_columns", "WARN", extra=extra)
    else:
        vlog.event("schema_columns", "PASS")


def apply_row_checks(df: DataFrame, cfg: dict, vlog: ValidationLog) -> tuple[DataFrame, DataFrame]:
    """Cast to target types and split rows into (valid, rejected).

    Rejected rows carry a _dq_reason column and are written to quarantine by
    the caller - nothing is silently dropped.
    """
    dq = cfg["data_quality"]

    # try_cast, not cast: under ANSI mode a plain cast throws on malformed
    # input (e.g. tower_id='ABC'), but here a failed cast is exactly the
    # signal we want - it becomes NULL and the row gets rejected with a reason.
    typed = (
        df.withColumn("tower_id_t", F.col("tower_id").try_cast(IntegerType()))
        .withColumn("timestamp_t", F.col("timestamp").try_cast(LongType()))
        .withColumn("signal_strength_t", F.col("signal_strength").try_cast(DoubleType()))
        .withColumn("data_volume_mb_t", F.col("data_volume_mb").try_cast(DoubleType()))
    )

    # Rules are evaluated together so a row reports every problem it has,
    # not just the first one.
    rules = {
        "tower_id_not_castable": F.col("tower_id").isNotNull() & F.col("tower_id_t").isNull(),
        "tower_id_null": F.col("tower_id").isNull(),
        "region_null": F.col("region").isNull() | (F.trim(F.col("region")) == ""),
        "timestamp_invalid": F.col("timestamp_t").isNull(),
        "signal_strength_null": F.col("signal_strength_t").isNull(),
        "signal_strength_out_of_range": F.col("signal_strength_t").isNotNull()
        & ~F.col("signal_strength_t").between(dq["signal_strength_min"], dq["signal_strength_max"]),
        "data_volume_null": F.col("data_volume_mb_t").isNull(),
        "data_volume_negative": F.col("data_volume_mb_t") < dq["data_volume_min"],
    }
    reason = F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    checked = typed.withColumn("_dq_reason", reason)

    rejected = checked.filter(F.col("_dq_reason") != "")
    valid = checked.filter(F.col("_dq_reason") == "").drop("_dq_reason")

    total, n_rejected = df.count(), rejected.count()
    for row in rejected.groupBy("_dq_reason").count().collect():
        vlog.event("row_checks", "REJECTED", reason=row["_dq_reason"], rows=row["count"])
    vlog.event(
        "row_checks", "PASS" if n_rejected == 0 else "WARN",
        total_rows=total, rejected_rows=n_rejected,
    )

    if total > 0 and n_rejected / total > dq["max_rejected_fraction"]:
        vlog.event(
            "rejected_fraction", "FAIL",
            fraction=round(n_rejected / total, 3), threshold=dq["max_rejected_fraction"],
        )
        raise DataQualityError(
            f"{n_rejected}/{total} rows rejected - above the "
            f"{dq['max_rejected_fraction']:.0%} threshold, stopping the run"
        )
    return valid, rejected


def apply_dataset_checks(df: DataFrame, cfg: dict, vlog: ValidationLog) -> DataFrame:
    """Statistical checks on the valid rows. Flags, never drops."""
    dq = cfg["data_quality"]

    dup_count = df.count() - df.dropDuplicates(["tower_id_t", "region", "timestamp_t"]).count()
    vlog.event("duplicates", "PASS" if dup_count == 0 else "WARN", duplicate_rows=dup_count)

    # Towers reporting from more than one region. Widespread in the sample
    # feed, so treated as a known quirk (see README assumptions) - flagged
    # for ops, not rejected.
    multi_region = (
        df.groupBy("tower_id_t")
        .agg(F.countDistinct("region").alias("n_regions"))
        .filter(F.col("n_regions") > 1)
    )
    n_multi = multi_region.count()
    vlog.event(
        "tower_region_consistency", "PASS" if n_multi == 0 else "WARN",
        towers_in_multiple_regions=n_multi,
    )

    # Volume outliers per region via z-score.
    stats = df.groupBy("region").agg(
        F.avg("data_volume_mb_t").alias("mu"), F.stddev("data_volume_mb_t").alias("sigma")
    )
    flagged = (
        df.join(stats, "region")
        .withColumn(
            "_volume_anomaly",
            F.when(
                (F.col("sigma") > 0)
                & (
                    F.abs((F.col("data_volume_mb_t") - F.col("mu")) / F.col("sigma"))
                    > dq["anomaly_zscore_threshold"]
                ),
                True,
            ).otherwise(False),
        )
        .drop("mu", "sigma")
    )
    n_anomalies = flagged.filter("_volume_anomaly").count()
    vlog.event(
        "volume_anomaly_zscore", "PASS" if n_anomalies == 0 else "WARN",
        anomalous_rows=n_anomalies, threshold=dq["anomaly_zscore_threshold"],
    )
    return flagged


def write_quarantine(rejected: DataFrame, cfg: dict, run_date: str) -> None:
    if rejected.isEmpty():
        return
    quarantine_path = f"{cfg['paths']['quarantine']}/network_metrics/_run_date={run_date}"
    (
        rejected.drop("tower_id_t", "timestamp_t", "signal_strength_t", "data_volume_mb_t")
        .write.mode("overwrite")
        .option("header", True)
        .csv(quarantine_path)
    )
