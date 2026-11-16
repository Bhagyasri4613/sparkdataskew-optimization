import csv
import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional

import pytest

pytest.importorskip("pyspark", reason="Spark tests require PySpark.")

from pyspark.sql import SparkSession

from src.application.benchmarking import benchmark_aggregations
from src.application.ingestion import (
    RAW_SCHEMA,
    TRANSACTION_COLUMNS,
    _validate_csv_header,
)
from src.application.validation import validate_transactions
from src.config import load_config
from src.application.pipeline import run_pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SUPPORTED_CURRENCIES = ["USD", "EUR"]


def _raw_frame(spark: SparkSession, *rows: Dict[str, Optional[str]]):
    values = [
        tuple(row.get(column) for column in TRANSACTION_COLUMNS)
        + (row.get("_corrupt_record"),)
        for row in rows
    ]
    return spark.createDataFrame(values, RAW_SCHEMA)


def _valid_row(transaction_id: str = "tx-1", **overrides: str):
    row = {
        "transaction_id": transaction_id,
        "customer_id": "customer-1",
        "transaction_ts": "2025-01-01T10:00:00",
        "amount": "10.00",
        "currency": "USD",
        "merchant_id": "merchant-1",
        "_corrupt_record": None,
    }
    row.update(overrides)
    return row


def test_happy_path_reconciles_normal_and_salted_aggregations(spark: SparkSession):
    raw = _raw_frame(
        spark,
        _valid_row("tx-1"),
        _valid_row("tx-2", amount="2.50"),
    )
    accepted, rejected = validate_transactions(raw, SUPPORTED_CURRENCIES)

    normal, salted, report = benchmark_aggregations(
        accepted, salt_buckets=4, repeats=1
    )

    assert accepted.count() == 2
    assert rejected.count() == 0
    assert report["results_match"] is True
    assert normal.collect()[0]["total_amount"] == Decimal("12.50")
    assert salted.collect()[0]["transaction_count"] == 2


def test_empty_input_produces_empty_outputs(spark: SparkSession):
    raw = spark.createDataFrame([], RAW_SCHEMA)

    accepted, rejected = validate_transactions(raw, SUPPORTED_CURRENCIES)

    assert accepted.count() == 0
    assert rejected.count() == 0


def test_malformed_timestamp_and_amount_are_quarantined(spark: SparkSession):
    raw = _raw_frame(
        spark,
        _valid_row("bad-time", transaction_ts="not-a-time"),
        _valid_row("bad-amount", amount="not-money"),
        _valid_row("corrupt", _corrupt_record="broken source row"),
    )

    accepted, rejected = validate_transactions(raw, SUPPORTED_CURRENCIES)

    reasons = {row["rejection_reason"] for row in rejected.collect()}
    assert accepted.count() == 0
    assert len(reasons) == 3
    assert "invalid_transaction_timestamp" in reasons
    assert "invalid_amount" in reasons
    assert "malformed_record" in reasons


def test_duplicate_transaction_id_keeps_one_deterministic_row(spark: SparkSession):
    raw = _raw_frame(
        spark,
        _valid_row("same-id", amount="20.00"),
        _valid_row("same-id", amount="10.00"),
    )

    accepted, rejected = validate_transactions(raw, SUPPORTED_CURRENCIES)

    assert accepted.count() == 1
    assert accepted.collect()[0]["amount"] == Decimal("10.00")
    assert [row["rejection_reason"] for row in rejected.collect()] == [
        "duplicate_transaction_id"
    ]


def test_null_required_field_is_rejected(spark: SparkSession):
    raw = _raw_frame(spark, _valid_row("null-customer", customer_id=None))

    accepted, rejected = validate_transactions(raw, SUPPORTED_CURRENCIES)

    assert accepted.count() == 0
    assert "missing_customer_id" in rejected.collect()[0]["rejection_reason"]


def test_zero_and_negative_amounts_are_rejected(spark: SparkSession):
    raw = _raw_frame(
        spark,
        _valid_row("zero", amount="0.00"),
        _valid_row("negative", amount="-0.01"),
    )

    accepted, rejected = validate_transactions(raw, SUPPORTED_CURRENCIES)

    assert accepted.count() == 0
    assert all(
        row["rejection_reason"] == "non_positive_amount"
        for row in rejected.collect()
    )


def test_unsupported_currency_is_rejected(spark: SparkSession):
    raw = _raw_frame(spark, _valid_row("bad-currency", currency="XYZ"))

    accepted, rejected = validate_transactions(raw, SUPPORTED_CURRENCIES)

    assert accepted.count() == 0
    assert "unsupported_currency" in rejected.collect()[0]["rejection_reason"]


def test_out_of_order_transactions_are_kept(spark: SparkSession):
    raw = _raw_frame(
        spark,
        _valid_row("later-event", transaction_ts="2025-01-03T10:00:00"),
        _valid_row("earlier-event", transaction_ts="2025-01-01T10:00:00"),
    )

    accepted, rejected = validate_transactions(raw, SUPPORTED_CURRENCIES)

    assert accepted.count() == 2
    assert rejected.count() == 0


def test_changed_csv_header_fails_clearly(tmp_path: Path):
    input_path = tmp_path / "changed.csv"
    input_path.write_text(
        "transaction_id,customer_id,unexpected,amount,currency,merchant_id\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="CSV header must be exactly"):
        _validate_csv_header(input_path)


def test_sample_pipeline_matches_separate_expected_output(
    spark: SparkSession, tmp_path: Path
):
    config = deepcopy(load_config(PROJECT_ROOT / "config.yaml"))
    config["spark"]["master"] = "local[2]"
    config["benchmark"]["repeats"] = 1
    config["data"]["output_dir"] = str(tmp_path / "output")
    config["data"]["generated_dir"] = str(tmp_path / "generated")
    config["logging"]["directory"] = str(tmp_path / "logs")

    metrics = run_pipeline(
        config=config,
        project_root=PROJECT_ROOT,
        input_path=Path("data/input/sample_transactions.csv"),
        run_id="sample-test",
    )

    with (tmp_path / "output/current/normal_aggregation.csv").open(
        encoding="utf-8", newline=""
    ) as actual_file:
        actual = [
            (
                row["customer_id"],
                int(row["transaction_count"]),
                Decimal(row["total_amount"]),
            )
            for row in csv.DictReader(actual_file)
        ]
    with (PROJECT_ROOT / "data/expected/sample_customer_aggregates.csv").open(
        encoding="utf-8", newline=""
    ) as expected_file:
        expected = [
            (
                row["customer_id"],
                int(row["transaction_count"]),
                Decimal(row["total_amount"]),
            )
            for row in csv.DictReader(expected_file)
        ]

    assert metrics["status"] == "success"
    assert metrics["input_count"] == 12
    assert metrics["accepted_count"] == 3
    assert metrics["rejected_count"] == 9
    assert metrics["results_match"] is True
    assert metrics["count_reconciled"] is True
    assert actual == expected
    assert (tmp_path / "output/current/rejected.jsonl").is_file()
    assert (tmp_path / "logs/sample-test.log").is_file()
    with (tmp_path / "output/current/rejected.jsonl").open(
        encoding="utf-8"
    ) as rejected_file:
        rejected = [json.loads(line) for line in rejected_file]
    assert all(row["source_file"] and row["run_id"] for row in rejected)

    retry_metrics = run_pipeline(
        config=config,
        project_root=PROJECT_ROOT,
        input_path=Path("data/input/sample_transactions.csv"),
        run_id="sample-test",
    )
    assert retry_metrics["input_count"] == metrics["input_count"]
    assert retry_metrics["accepted_count"] == metrics["accepted_count"]
    assert retry_metrics["rejected_count"] == metrics["rejected_count"]


def test_pipeline_records_failure_reason(tmp_path: Path):
    config = deepcopy(load_config(PROJECT_ROOT / "config.yaml"))
    config["spark"]["master"] = "local[2]"
    config["data"]["output_dir"] = str(tmp_path / "output")
    config["data"]["generated_dir"] = str(tmp_path / "generated")
    config["logging"]["directory"] = str(tmp_path / "logs")

    with pytest.raises(FileNotFoundError):
        run_pipeline(
            config=config,
            project_root=PROJECT_ROOT,
            input_path=tmp_path / "missing.csv",
            run_id="missing-input-test",
        )

    with (tmp_path / "output/current/pipeline_metrics.json").open(
        encoding="utf-8"
    ) as metrics_file:
        metrics = json.load(metrics_file)
    assert metrics["status"] == "failed"
    assert "FileNotFoundError" in metrics["failure_reason"]
