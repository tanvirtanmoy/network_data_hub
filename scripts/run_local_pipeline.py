#!/usr/bin/env python
"""Run the pipeline locally for one day, outside Airflow.

Usage:
    python scripts/run_local_pipeline.py --date 20250723

Mirrors the Airflow DAG task by task so the pipeline can be developed and
reviewed without a running Airflow instance. Silver and gold steps get wired
in here as they are implemented.
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import load_config
from src.ingestion import ingest_to_bronze
from src.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("run_local_pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the daily network metrics pipeline")
    parser.add_argument("--date", required=True, help="run date, YYYYMMDD")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)
    try:
        logger.info("=== bronze: ingesting raw file for %s ===", args.date)
        ingest_to_bronze(spark, cfg, args.date)
        # TODO: silver (validate + clean) and gold (aggregates) steps
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
