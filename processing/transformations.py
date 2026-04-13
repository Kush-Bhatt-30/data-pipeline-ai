from __future__ import annotations

import logging
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)


TX_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType(), nullable=False),
        T.StructField("event_time", T.StringType(), nullable=False),
        T.StructField("customer_id", T.StringType(), nullable=False),
        T.StructField("amount", T.DoubleType(), nullable=False),
        T.StructField("currency", T.StringType(), nullable=False),
        T.StructField("item_count", T.IntegerType(), nullable=False),
        T.StructField("payment_method", T.StringType(), nullable=False),
        T.StructField("country", T.StringType(), nullable=False),
    ]
)


def read_raw_transactions_jsonl(spark: SparkSession, paths: Iterable[str]) -> DataFrame:
    paths = list(paths)
    if not paths:
        raise ValueError("No input paths provided")
    logger.info("Reading raw transactions JSONL from %d path(s)", len(paths))
    return spark.read.schema(TX_SCHEMA).json(paths)


def clean_and_enrich(df: DataFrame) -> DataFrame:
    """
    Cleans raw data and adds:
      - event_ts (timestamp)
      - dt (yyyy-mm-dd string)
      - ingest_ts (timestamp)
    """
    out = (
        df.withColumn("event_ts", F.to_timestamp(F.col("event_time")))
        .withColumn("dt", F.date_format(F.to_date(F.col("event_ts")), "yyyy-MM-dd"))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    # Basic sanity checks / corrections
    out = out.withColumn("amount", F.when(F.col("amount") < 0, None).otherwise(F.col("amount")))
    out = out.withColumn(
        "item_count", F.when(F.col("item_count") <= 0, None).otherwise(F.col("item_count"))
    )

    # Drop invalid records
    out = out.dropna(
        subset=[
            "transaction_id",
            "event_ts",
            "customer_id",
            "amount",
            "currency",
            "item_count",
            "payment_method",
            "country",
            "dt",
        ]
    )

    # Dedupe within the batch/day
    out = out.dropDuplicates(["transaction_id"])
    return out


class DataContractValidator:
    """
    Enterprise-grade Data Contract Validator.
    Mimics Great Expectations (GX) interface but native to PySpark.
    Fail-fast mechanism prevents bad data from reaching the warehouse.
    """

    def __init__(self, df: DataFrame):
        self.df = df
        self.errors: list[str] = []

    def expect_table_row_count_to_be_between(
        self, min_value: int, max_value: int | None = None
    ) -> DataContractValidator:
        cnt = self.df.count()
        if min_value is not None and cnt < min_value:
            self.errors.append(f"Row count {cnt} is less than minimum required {min_value}")
        if max_value is not None and cnt > max_value:
            self.errors.append(f"Row count {cnt} is greater than maximum allowed {max_value}")
        return self

    def expect_column_values_to_not_be_null(self, column: str) -> DataContractValidator:
        null_cnt = self.df.where(F.col(column).isNull()).count()
        if null_cnt > 0:
            self.errors.append(f"Column '{column}' contains {null_cnt} null values")
        return self

    def expect_column_values_to_be_in_set(
        self, column: str, value_set: list[str]
    ) -> DataContractValidator:
        invalid_cnt = self.df.where(~F.col(column).isin(value_set)).count()
        if invalid_cnt > 0:
            self.errors.append(
                f"Column '{column}' contains {invalid_cnt} values not in permitted set {value_set}"
            )
        return self

    def expect_column_values_to_be_between(
        self, column: str, min_v: float, max_v: float
    ) -> DataContractValidator:
        invalid_cnt = self.df.where((F.col(column) < min_v) | (F.col(column) > max_v)).count()
        if invalid_cnt > 0:
            self.errors.append(
                f"Column '{column}' contains {invalid_cnt} values outside range [{min_v}, {max_v}]"
            )
        return self

    def validate(self) -> None:
        if self.errors:
            error_msg = "DATA CONTRACT VIOLATION(S) DETECTED:\n" + "\n".join(
                f"- {e}" for e in self.errors
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        logger.info("All Data Contract Expectations passed successfully!")


def aggregate_daily(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("dt", "country", "payment_method")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.sum("item_count").alias("total_items"),
        )
        .withColumn("avg_amount", F.round(F.col("avg_amount"), 2))
        .withColumn("total_amount", F.round(F.col("total_amount"), 2))
    )


def write_parquet_partitioned(
    df: DataFrame,
    *,
    base_path: str,
    partition_cols: Sequence[str],
    mode: str = "append",
) -> None:
    logger.info(
        "Writing Parquet: %s (mode=%s partitions=%s)", base_path, mode, list(partition_cols)
    )
    (
        df.write.mode(mode)
        .partitionBy(*partition_cols)
        .option("compression", "snappy")
        .parquet(base_path)
    )
