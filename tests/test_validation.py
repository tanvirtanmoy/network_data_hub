"""Tests for the DQ gate. Run with: python -m pytest tests/ -q"""

import copy

import pytest
from pyspark.sql import SparkSession

from src import validation
from src.config import load_config

COLUMNS = ["tower_id", "region", "timestamp", "signal_strength", "data_volume_mb"]

CLEAN_ROWS = [
    ("101", "North", "1753315200", "-76.5", "99.0"),
    ("102", "South", "1753316100", "-81.2", "150.3"),
    ("103", "East", "1753317000", "-90.0", "55.7"),
    ("104", "West", "1753317900", "-70.1", "240.8"),
    ("105", "Central", "1753318800", "-85.5", "120.0"),
    ("106", "North", "1753319700", "-79.9", "88.8"),
    ("107", "South", "1753320600", "-88.3", "199.9"),
    ("108", "East", "1753321500", "-73.4", "60.2"),
    ("101", "West", "1753322400", "-92.7", "175.5"),
    ("102", "Central", "1753323300", "-80.0", "140.0"),
]


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("validation-tests")
        .config("spark.sql.shuffle.partitions", 2)
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def cfg(tmp_path):
    cfg = copy.deepcopy(load_config())
    cfg["paths"]["logs"] = str(tmp_path)  # keep test runs out of the real logs/
    return cfg


@pytest.fixture
def vlog(cfg):
    return validation.ValidationLog(cfg, "20250724")


def _reasons(rejected_df) -> dict[str, str]:
    """map tower_id -> _dq_reason for easy asserts"""
    return {r["tower_id"]: r["_dq_reason"] for r in rejected_df.collect()}


def test_bad_rows_rejected_with_reasons(spark, cfg, vlog):
    rows = CLEAN_ROWS + [
        ("201", "North", "1753324200", "-150.0", "50.0"),   # signal too low
        ("202", "South", "1753325100", "10.0", "50.0"),     # positive dBm
        (None, "East", "1753326000", "-80.0", "50.0"),      # null tower
        ("204", "West", "1753326900", "-80.0", "-1.0"),     # negative volume
        ("205", "Central", "oops", "-80.0", "50.0"),        # bad timestamp
    ]
    df = spark.createDataFrame(rows, COLUMNS)
    # 5 bad rows out of 15 would trip the hard-stop threshold; this test is
    # about the reject reasons, so loosen it here
    cfg["data_quality"]["max_rejected_fraction"] = 0.5
    valid, rejected = validation.apply_row_checks(df, cfg, vlog)

    assert valid.count() == len(CLEAN_ROWS)
    reasons = _reasons(rejected)
    assert reasons["201"] == "signal_strength_out_of_range"
    assert reasons["202"] == "signal_strength_out_of_range"
    assert reasons[None] == "tower_id_null"
    assert reasons["204"] == "data_volume_negative"
    assert reasons["205"] == "timestamp_invalid"


def test_clean_rows_all_pass(spark, cfg, vlog):
    df = spark.createDataFrame(CLEAN_ROWS, COLUMNS)
    valid, rejected = validation.apply_row_checks(df, cfg, vlog)
    assert valid.count() == len(CLEAN_ROWS)
    assert rejected.count() == 0
    assert "_dq_reason" not in valid.columns


def test_too_many_rejects_stops_the_run(spark, cfg, vlog):
    rows = CLEAN_ROWS[:2] + [
        ("301", "North", "1753324200", "-150.0", "50.0"),
        ("302", "South", "1753325100", "-160.0", "50.0"),
        ("303", "East", "1753326000", "-170.0", "50.0"),
    ]
    df = spark.createDataFrame(rows, COLUMNS)  # 3 of 5 bad, way over 10%
    with pytest.raises(validation.DataQualityError, match="rejected"):
        validation.apply_row_checks(df, cfg, vlog)


def test_bad_file_name_fails(cfg, vlog):
    with pytest.raises(validation.DataQualityError, match="convention"):
        validation.validate_file("metrics-2025.csv", COLUMNS, cfg, vlog)


def test_missing_column_fails(cfg, vlog):
    cols = [c for c in COLUMNS if c != "signal_strength"]
    with pytest.raises(validation.DataQualityError, match="missing"):
        validation.validate_file("network_metrics_20250724.csv", cols, cfg, vlog)
