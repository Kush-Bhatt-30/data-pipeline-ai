from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pyspark.sql import SparkSession

from config.logging import configure_logging
from config.settings import load_settings
from processing.transformations import (
    DataContractValidator,
    aggregate_daily,
    clean_and_enrich,
    read_raw_transactions_jsonl,
)
from storage.s3_client import ensure_buckets, env_s3_buckets, upload_directory

logger = logging.getLogger(__name__)


def _spark_session(master_url: str, app_name: str) -> SparkSession:
    """
    Initializes a highly fault-tolerant PySpark Session.

    Configures critical dynamic partition overwrite modes and strictly
    binds the executor timezone to UTC to prevent disjointed analytics
    across distributed server clusters.
    """
    builder = SparkSession.builder.appName(app_name)
    if master_url:
        builder = builder.master(master_url)

    # Conservative Spark defaults for local docker clusters
    builder = (
        builder.config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    return builder.getOrCreate()


def _update_state_file(state_path: Path, dt: str) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "last_processed_dt": dt,
        "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    state_path.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def _resolve_input_paths_local(raw_root: Path, dt: str) -> List[str]:
    # raw_mirror/{dataset}/dt=YYYY-MM-DD/source=.../*.jsonl
    base = raw_root / "transactions" / f"dt={dt}"
    if not base.exists():
        return []
    return [str(p) for p in base.rglob("*.jsonl") if p.is_file()]


def run(dt: str) -> None:
    """
    Executes the PySpark Lambda batching operation for a single partition.

    Performs data cleansing, strictly enforces data contracts, and commits
    parquets to the analytical warehouse. Designed to be safely idempotent
    via dynamic partitioning.

    Args:
        dt (str): The isolated temporal partition (YYYY-MM-DD) to process.
    """
    s = load_settings()
    configure_logging(str(s.get("app.log_level", "INFO")))

    master_url = str(s.get("spark.master_url", ""))
    app_name = str(s.get("spark.app_name", "dpa-spark"))
    io_mode = str(s.get("spark.io_mode", "local_mirror"))
    output_format = str(s.get("spark.output_format", "parquet")).lower()

    spark = _spark_session(master_url, app_name)
    try:
        if io_mode != "local_mirror":
            raise NotImplementedError(
                "spark.io_mode=s3a is not enabled in this local-ready template. "
                "Use local_mirror for the runnable demo, or extend Spark with S3A jars and enable s3a paths."
            )

        raw_dir = s.resolve_path(str(s.get("spark.local_mirror.raw_dir")))
        processed_dir = s.resolve_path(str(s.get("spark.local_mirror.processed_dir")))
        bi_dir = s.resolve_path(str(s.get("spark.local_mirror.bi_dir")))
        warehouse_dir = s.resolve_path(str(s.get("spark.local_mirror.warehouse_dir")))
        state_path = s.resolve_path("storage/local/metadata/processed_state.json")

        input_paths = _resolve_input_paths_local(raw_dir, dt)
        if not input_paths:
            raise FileNotFoundError(f"No raw inputs found for dt={dt} under {raw_dir}")

        raw_df = read_raw_transactions_jsonl(spark, input_paths)
        clean_df = clean_and_enrich(raw_df)

        # Ensure we're processing only the requested dt partition
        # Data Contract Validation (Fail-fast before writing partitioned Parquet)
        (
            DataContractValidator(clean_df)
            .expect_table_row_count_to_be_between(min_value=1)
            .expect_column_values_to_not_be_null("amount")
            .expect_column_values_to_not_be_null("customer_id")
            .expect_column_values_to_be_in_set("currency", ["USD", "EUR", "GBP", "JPY"])
            .expect_column_values_to_be_between("amount", min_v=0.01, max_v=999999.0)
            .validate()
        )

        # Write processed, partitioned by dt (incremental)
        out_processed = processed_dir / "transactions"
        writer = clean_df.write.mode("append").partitionBy("dt").option("compression", "snappy")
        if output_format == "parquet":
            writer.parquet(str(out_processed))
        elif output_format == "delta":
            # Delta support is optional. For production, install delta-spark and configure Spark with delta extensions.
            try:
                writer.format("delta").save(str(out_processed))
            except Exception as e:
                raise RuntimeError(
                    "Delta output requested but Delta Lake isn't configured. "
                    "Set spark.output_format=parquet for the local demo, or add delta-spark + Spark delta extensions."
                ) from e
        else:
            raise ValueError(f"Unsupported spark.output_format: {output_format}")
        logger.info("Processed Parquet written to %s", out_processed)

        # BI-friendly daily aggregates
        daily_df = aggregate_daily(clean_df)
        out_bi = bi_dir / "daily_metrics"
        daily_df.write.mode("append").partitionBy("dt").option("compression", "snappy").parquet(
            str(out_bi)
        )
        logger.info("BI metrics written to %s", out_bi)

        # Mock "warehouse" export: a single CSV snapshot per dt
        out_wh_dt = warehouse_dir / f"dt={dt}" / "daily_metrics_csv"
        out_wh_dt.parent.mkdir(parents=True, exist_ok=True)
        daily_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(out_wh_dt))
        logger.info("Mock warehouse export written to %s", out_wh_dt)

        _update_state_file(state_path, dt)

        # Optional: upload processed/bi/warehouse to S3 "processed"/"warehouse" buckets for completeness
        buckets = env_s3_buckets()
        try:
            ensure_buckets(buckets.values())
            uploaded = upload_directory(out_processed, buckets["processed"], "transactions/")
            logger.info(
                "Uploaded %d processed file(s) to S3 bucket=%s", uploaded, buckets["processed"]
            )
            uploaded = upload_directory(out_bi, buckets["processed"], "bi/daily_metrics/")
            logger.info("Uploaded %d BI file(s) to S3 bucket=%s", uploaded, buckets["processed"])
            uploaded = upload_directory(
                warehouse_dir / f"dt={dt}", buckets["warehouse"], f"dt={dt}/"
            )
            logger.info(
                "Uploaded %d warehouse file(s) to S3 bucket=%s", uploaded, buckets["warehouse"]
            )
        except Exception as e:
            logger.warning("S3 upload skipped/failed (local run still OK): %s", e)
    finally:
        spark.stop()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Incremental Spark job: raw -> processed + BI exports."
    )
    parser.add_argument("--dt", default=datetime.now(timezone.utc).date().isoformat())
    args = parser.parse_args(argv)
    run(str(args.dt))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
