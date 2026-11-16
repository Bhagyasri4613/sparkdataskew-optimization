"""Transaction schema and data-quality validation."""

from typing import Iterable, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.application.ingestion import CORRUPT_RECORD_COLUMN, TRANSACTION_COLUMNS


PROVENANCE_COLUMNS = ("source_file", "run_id")
REJECTED_COLUMNS = TRANSACTION_COLUMNS + PROVENANCE_COLUMNS + (
    "source_record",
    "rejection_reason",
)


def validate_transactions(
    raw: DataFrame, supported_currencies: Iterable[str]
) -> Tuple[DataFrame, DataFrame]:
    """Return typed accepted rows and raw rejected rows with explicit reasons."""
    for column in TRANSACTION_COLUMNS:
        if column not in raw.columns:
            raw = raw.withColumn(column, F.lit(None).cast("string"))
    if CORRUPT_RECORD_COLUMN not in raw.columns:
        raw = raw.withColumn(CORRUPT_RECORD_COLUMN, F.lit(None).cast("string"))
    provenance_columns = [
        column for column in PROVENANCE_COLUMNS if column in raw.columns
    ]

    normalized = raw.select(
        *[
            F.trim(F.col(column).cast("string")).alias(column)
            for column in TRANSACTION_COLUMNS
        ],
        F.col(CORRUPT_RECORD_COLUMN).cast("string").alias(CORRUPT_RECORD_COLUMN),
        *[F.col(column).cast("string").alias(column) for column in provenance_columns],
    )
    prepared = (
        normalized.withColumn("_parsed_ts", F.to_timestamp(F.col("transaction_ts")))
        .withColumn("_parsed_amount", F.col("amount").cast("decimal(18,2)"))
    )

    currencies = [currency.upper() for currency in supported_currencies]
    checks = [
        (F.col(CORRUPT_RECORD_COLUMN).isNotNull(), "malformed_record"),
        (
            F.col("transaction_id").isNull() | (F.col("transaction_id") == ""),
            "missing_transaction_id",
        ),
        (
            F.col("customer_id").isNull() | (F.col("customer_id") == ""),
            "missing_customer_id",
        ),
        (F.col("_parsed_ts").isNull(), "invalid_transaction_timestamp"),
        (F.col("_parsed_amount").isNull(), "invalid_amount"),
        (F.col("_parsed_amount") <= F.lit(0), "non_positive_amount"),
        (
            ~F.upper(F.col("currency")).isin(currencies)
            | F.col("currency").isNull()
            | (F.col("currency") == ""),
            "unsupported_currency",
        ),
        (
            F.col("merchant_id").isNull() | (F.col("merchant_id") == ""),
            "missing_merchant_id",
        ),
    ]
    rejection_expression = F.concat_ws(
        "|", *[F.when(condition, F.lit(reason)) for condition, reason in checks]
    )
    prepared = prepared.withColumn("_pre_rejection", rejection_expression)

    invalid = prepared.filter(F.col("_pre_rejection") != "").select(
        *[F.col(column) for column in TRANSACTION_COLUMNS],
        *[F.col(column) for column in provenance_columns],
        F.col(CORRUPT_RECORD_COLUMN).alias("source_record"),
        F.col("_pre_rejection").alias("rejection_reason"),
    )

    ordering_columns = (
        "customer_id",
        "transaction_ts",
        "amount",
        "currency",
        "merchant_id",
    ) + tuple(provenance_columns)
    ordering = [F.col(column).asc() for column in ordering_columns]
    candidates = prepared.filter(F.col("_pre_rejection") == "")
    ranked = candidates.withColumn(
        "_duplicate_rank",
        F.row_number().over(Window.partitionBy("transaction_id").orderBy(*ordering)),
    )
    accepted = ranked.filter(F.col("_duplicate_rank") == 1).select(
        F.col("transaction_id"),
        F.col("customer_id"),
        F.col("_parsed_ts").alias("transaction_ts"),
        F.col("_parsed_amount").alias("amount"),
        F.upper(F.col("currency")).alias("currency"),
        F.col("merchant_id"),
        *[F.col(column) for column in provenance_columns],
    )
    duplicates = ranked.filter(F.col("_duplicate_rank") > 1).select(
        *[F.col(column) for column in TRANSACTION_COLUMNS],
        *[F.col(column) for column in provenance_columns],
        F.col(CORRUPT_RECORD_COLUMN).alias("source_record"),
        F.lit("duplicate_transaction_id").alias("rejection_reason"),
    )
    rejected = invalid.unionByName(duplicates).select(
        *TRANSACTION_COLUMNS,
        *provenance_columns,
        "source_record",
        "rejection_reason",
    )
    return accepted, rejected
