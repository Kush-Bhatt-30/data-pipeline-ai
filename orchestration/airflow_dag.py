from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from ai.model_training import train_and_save
from ai.prediction import batch_predict
from config.logging import configure_logging
from config.settings import load_settings
from ingestion.api_ingestion import (
    fetch_from_rest_api,
    read_transactions_from_csv,
    read_transactions_from_jsonl,
    upload_raw_transactions,
)
from processing.spark_job import run as spark_run
from storage.s3_client import ensure_buckets, env_s3_buckets

logger = logging.getLogger(__name__)


def failure_alert(context):
    """
    Mock Slack/PagerDuty alert on task failure.
    """
    task_instance = context.get("task_instance")
    logger.error(
        "🚨 ALERT: Task %s failed in DAG %s. Triggering PagerDuty/Slack...",
        task_instance.task_id,
        task_instance.dag_id,
    )


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_alert,
}


@dag(
    dag_id="data_pipeline_ai_lambda",
    description="End-to-end batch + AI pipeline (raw -> processed -> model -> predictions).",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["data-pipeline-ai", "lambda-architecture"],
)
def data_pipeline_ai_lambda():
    """
    Main entry point for the Lambda Architecture ingestion orchestrator.

    This DAG enforces an idempotent, day-partitioned batch pipeline.
    It provisions necessary S3 storage abstractions natively via LocalStack,
    pulls from upstream API endpoints, triggers the fault-tolerant PySpark
    validation/processing engine, and publishes the ML output artifacts.
    """

    @task
    def bootstrap_storage() -> None:
        s = load_settings()
        configure_logging(str(s.get("app.log_level", "INFO")))
        buckets = env_s3_buckets()
        ensure_buckets(buckets.values())

    @task
    def ingest_rest(ds: str) -> None:
        """
        Executes outbound micro-batch extraction from the primary REST generic endpoint.
        Uses logical execution dates (ds) mapped organically to the Spark Partitioner.
        """
        s = load_settings()
        configure_logging(str(s.get("app.log_level", "INFO")))
        url = str(s.get("sources.rest_api_url"))
        txs = fetch_from_rest_api(url)
        upload_raw_transactions(txs, dt=ds, source="api")

    @task
    def ingest_files(ds: str) -> None:
        s = load_settings()
        configure_logging(str(s.get("app.log_level", "INFO")))
        sample_dir = s.resolve_path(str(s.get("sources.batch_files_dir")))
        txs = []
        csv_path = sample_dir / "transactions.csv"
        jsonl_path = sample_dir / "transactions.jsonl"
        if csv_path.exists():
            txs.extend(read_transactions_from_csv(csv_path))
        if jsonl_path.exists():
            txs.extend(read_transactions_from_jsonl(jsonl_path))
        if not txs:
            raise AirflowFailException(f"No batch files found in {sample_dir}")
        upload_raw_transactions(txs, dt=ds, source="files")

    @task
    def process_spark(ds: str) -> None:
        # For local compose we run Spark in local[*] mode inside Airflow (set via env override in docker-compose).
        spark_run(ds)

    @task
    def train_model(ds: str) -> str:
        path = train_and_save(dt=ds)
        return str(path)

    @task
    def predict_batch(ds: str) -> str:
        p = batch_predict(dt=ds)
        return str(p)

    boot = bootstrap_storage()
    r = ingest_rest("{{ ds }}")
    f = ingest_files("{{ ds }}")
    p = process_spark("{{ ds }}")
    m = train_model("{{ ds }}")
    pr = predict_batch("{{ ds }}")

    boot >> [r, f] >> p >> m >> pr


data_pipeline_ai_lambda()
