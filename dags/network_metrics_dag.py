"""Daily network metrics DAG.

Runs once a day after the nightly tower upload: waits for the raw file to
appear, then bronze -> silver -> gold. Each layer is its own task so a
failure is visible at the exact step and can be retried in isolation.

This file lives on the Airflow scheduler where airflow (and the providers)
are installed - it is deliberately not imported by the local runner or the
tests.
"""

import json
import logging
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from src.config import load_config
from src.ingestion import ingest_to_bronze, locate_daily_file
from src.spark_session import get_spark
from src.transform_gold import build_gold
from src.transform_silver import build_silver

logger = logging.getLogger(__name__)


def notify_failure(context) -> None:
    """Send an incident notification when any task in the DAG fails.

    Posts to a Slack channel via webhook (stored as an Airflow Variable).
    default_args also has email_on_failure as a fallback channel, so an
    incident never depends on Slack alone being up.
    """
    ti = context["task_instance"]
    message = {
        "text": (
            f":red_circle: *{context['dag'].dag_id}* failed\n"
            f"task: `{ti.task_id}` | run: {context['ds']}\n"
            f"log: {ti.log_url}"
        )
    }
    try:
        webhook = Variable.get("slack_dq_webhook")
        req = urllib.request.Request(
            webhook,
            data=json.dumps(message).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        # never let the notifier itself take the DAG down;
        # email_on_failure still fires
        logger.exception("slack notification failed")


def _file_is_there(**context) -> bool:
    # Local dev: poll the filesystem. In production this sensor is replaced
    # by an S3KeySensor on
    #   s3://raw-telecom-network-data/network_metrics_{{ ds_nodash }}.csv
    # (same contract: don't start processing until the object exists).
    cfg = load_config()
    try:
        locate_daily_file(cfg, context["ds_nodash"])
        return True
    except (FileNotFoundError, ValueError):
        return False


def _run_layer(layer_fn, **context) -> None:
    cfg = load_config()
    spark = get_spark(cfg)
    try:
        layer_fn(spark, cfg, context["ds_nodash"])
    finally:
        spark.stop()


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-eng-oncall@example.com"],
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="network_metrics_daily",
    description="Ingest daily tower metrics and build regional aggregates",
    schedule="0 6 * * *",  # towers upload overnight; 06:00 leaves headroom
    start_date=datetime(2025, 7, 23),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["network-metrics", "medallion"],
    doc_md=__doc__,
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_raw_file",
        python_callable=_file_is_there,
        poke_interval=300,
        timeout=60 * 60 * 4,  # give up after 4h and alert - file is overdue
        mode="reschedule",  # don't hold a worker slot while waiting
    )

    bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_run_layer,
        op_args=[ingest_to_bronze],
    )

    silver = PythonOperator(
        task_id="build_silver",
        python_callable=_run_layer,
        op_args=[build_silver],
    )

    gold = PythonOperator(
        task_id="build_gold",
        python_callable=_run_layer,
        op_args=[build_gold],
    )

    wait_for_file >> bronze >> silver >> gold
