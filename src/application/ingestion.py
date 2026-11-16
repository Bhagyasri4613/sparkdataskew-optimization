"""Input data ingestion."""

import csv
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType


TRANSACTION_COLUMNS = (
    "transaction_id",
    "customer_id",
    "transaction_ts",
    "amount",
    "currency",
    "merchant_id",
)
CORRUPT_RECORD_COLUMN = "_corrupt_record"
RAW_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in TRANSACTION_COLUMNS]
    + [StructField(CORRUPT_RECORD_COLUMN, StringType(), True)]
)


def read_transactions(
    spark: SparkSession, path: Path, input_format: Optional[str] = None
) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError("Input path not found: {}".format(path))

    file_format = (input_format or _format_from_path(path)).lower()
    reader = spark.read
    if file_format == "csv":
        if path.is_file():
            _validate_csv_header(path)
        return (
            reader.option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
            .schema(RAW_SCHEMA)
            .csv(str(path))
        )
    if file_format in ("json", "jsonl"):
        return (
            reader.option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
            .schema(RAW_SCHEMA)
            .json(str(path))
        )
    if file_format == "parquet":
        frame = reader.parquet(str(path))
        for column in TRANSACTION_COLUMNS:
            if column not in frame.columns:
                frame = frame.withColumn(column, lit(None).cast(StringType()))
        if CORRUPT_RECORD_COLUMN not in frame.columns:
            frame = frame.withColumn(
                CORRUPT_RECORD_COLUMN, lit(None).cast(StringType())
            )
        return frame.select(*TRANSACTION_COLUMNS, CORRUPT_RECORD_COLUMN)

    raise ValueError(
        "Unsupported input format '{}'; use CSV, JSON/JSONL, or Parquet.".format(
            file_format
        )
    )


def _validate_csv_header(path: Path) -> None:
    with path.open(encoding="utf-8-sig", newline="") as input_file:
        header = next(csv.reader(input_file), None)
    if header != list(TRANSACTION_COLUMNS):
        raise ValueError(
            "CSV header must be exactly: {}".format(",".join(TRANSACTION_COLUMNS))
        )


def _format_from_path(path: Path) -> str:
    suffix = path.suffix.lower().lstrip(".")
    if suffix in ("csv", "json", "jsonl", "parquet"):
        return suffix
    if path.is_dir():
        raise ValueError(
            "Input format is ambiguous for a directory; provide input_format explicitly."
        )
    raise ValueError("Cannot infer input format from path: {}".format(path))
