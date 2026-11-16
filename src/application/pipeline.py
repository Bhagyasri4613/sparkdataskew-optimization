"""Orchestration for the end-to-end Spark pipeline."""

import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from src.application.benchmarking import benchmark_aggregations
from src.application.data_generator import write_synthetic_transactions
from src.application.ingestion import read_transactions
from src.application.skew_detection import detect_customer_skew
from src.application.validation import validate_transactions
from src.presentation.reports import write_benchmark_report
from src.storage.io import write_frame, write_json_report


def run_pipeline(
    config: Dict[str, Any],
    project_root: Path,
    input_path: Optional[Path] = None,
    record_count: Optional[int] = None,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    settings = _validate_config(config, record_count, seed)
    run_id = run_id or uuid.uuid4().hex
    if not re.fullmatch(r"[A-Za-z0-9._-]+", run_id):
        raise ValueError("run_id may contain only letters, numbers, '.', '_' and '-'.")

    project_root = project_root.resolve()
    output_dir = _resolve_path(project_root, settings["data"]["output_dir"])
    log_dir = _resolve_path(project_root, settings["logging"]["directory"])
    current_output = output_dir / "current"
    log_dir.mkdir(parents=True, exist_ok=True)
    current_output.mkdir(parents=True, exist_ok=True)
    logger = _create_logger(
        log_dir / "{}.log".format(run_id),
        settings["logging"].get("level", "INFO"),
    )

    started_clock = time.perf_counter()
    metrics: Dict[str, Any] = {
        "run_id": run_id,
        "status": "running",
        "started_at": _utc_now(),
        "ended_at": None,
        "duration_seconds": None,
        "source_path": None,
        "output_path": str(current_output),
        "input_count": 0,
        "accepted_count": 0,
        "rejected_count": 0,
        "output_count": 0,
        "failure_reason": None,
        "results_match": None,
    }
    spark = None
    owns_spark = False
    accepted = None

    try:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        if input_path is not None:
            if not input_path.is_absolute():
                input_path = project_root / input_path
            input_path = input_path.resolve()
            metrics["source_path"] = str(input_path)
            if not input_path.exists():
                raise FileNotFoundError("Input path not found: {}".format(input_path))

        spark_settings = settings["spark"]
        existing_session = SparkSession.getActiveSession()
        spark = (
            SparkSession.builder.master(spark_settings["master"])
            .appName(spark_settings["app_name"])
            .config(
                "spark.sql.shuffle.partitions",
                str(spark_settings["shuffle_partitions"]),
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        owns_spark = existing_session is None

        if input_path is None:
            generator_settings = settings["generator"]
            input_path = (
                _resolve_path(project_root, settings["data"]["generated_dir"])
                / "transactions.csv"
            )
            write_synthetic_transactions(
                path=input_path,
                record_count=generator_settings["record_count"],
                seed=seed if seed is not None else generator_settings["random_seed"],
                customer_count=generator_settings["customer_count"],
                hot_customer_count=generator_settings["hot_customer_count"],
                hot_customer_fraction=generator_settings["hot_customer_fraction"],
            )

        input_path = input_path.resolve()
        metrics["source_path"] = str(input_path)
        logger.info("Starting run %s with input %s", run_id, input_path)

        raw = read_transactions(spark, input_path)
        raw = raw.withColumn("source_file", lit(input_path.name)).withColumn(
            "run_id", lit(run_id)
        )
        metrics["input_count"] = raw.count()
        accepted, rejected = validate_transactions(
            raw, settings["validation"]["supported_currencies"]
        )
        accepted = accepted.persist(StorageLevel.MEMORY_AND_DISK)
        metrics["accepted_count"] = accepted.count()
        metrics["rejected_count"] = rejected.count()
        metrics["count_reconciled"] = metrics["input_count"] == (
            metrics["accepted_count"] + metrics["rejected_count"]
        )
        if not metrics["count_reconciled"]:
            raise RuntimeError(
                "Input count does not equal accepted plus rejected counts."
            )

        skew_report = detect_customer_skew(
            accepted, top_n=settings["skew_detection"]["top_n"]
        )
        normal, salted, benchmark = benchmark_aggregations(
            accepted,
            salt_buckets=settings["optimization"]["salt_buckets"],
            repeats=settings["benchmark"]["repeats"],
        )
        benchmark["salt_buckets"] = settings["optimization"]["salt_buckets"]
        metrics.update(
            {
                "output_count": benchmark["output_count"],
                "results_match": benchmark["results_match"],
                "normal_median_seconds": benchmark["normal_median_seconds"],
                "salted_median_seconds": benchmark["salted_median_seconds"],
                "speedup_ratio": benchmark["speedup_ratio"],
                "skew": skew_report,
            }
        )

        write_frame(accepted, current_output / "accepted.csv", output_format="csv")
        write_frame(rejected, current_output / "rejected.jsonl", output_format="json")
        write_frame(
            normal.orderBy("customer_id"),
            current_output / "normal_aggregation.csv",
            output_format="csv",
        )
        write_frame(
            salted.orderBy("customer_id"),
            current_output / "salted_aggregation.csv",
            output_format="csv",
        )
        write_json_report(current_output / "skew_report.json", skew_report)
        write_benchmark_report(benchmark, current_output / "benchmark.csv")

        if not benchmark["results_match"]:
            raise AssertionError(
                "Normal and salted aggregation results did not reconcile."
            )

        metrics["status"] = "success"
        logger.info(
            "Run %s completed: input=%d accepted=%d rejected=%d groups=%d",
            run_id,
            metrics["input_count"],
            metrics["accepted_count"],
            metrics["rejected_count"],
            metrics["output_count"],
        )
        _finish_metrics(metrics, started_clock)
        write_json_report(current_output / "pipeline_metrics.json", metrics)
        return metrics
    except Exception as error:
        metrics["status"] = "failed"
        metrics["failure_reason"] = "{}: {}".format(type(error).__name__, error)
        _finish_metrics(metrics, started_clock)
        logger.exception("Run %s failed: %s", run_id, metrics["failure_reason"])
        write_json_report(current_output / "pipeline_metrics.json", metrics)
        raise
    finally:
        if accepted is not None:
            accepted.unpersist()
        if spark is not None and owns_spark:
            spark.stop()
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)


def _validate_config(
    config: Dict[str, Any],
    record_count_override: Optional[int],
    seed_override: Optional[int],
) -> Dict[str, Any]:
    section_names = (
        "spark",
        "data",
        "generator",
        "optimization",
        "benchmark",
        "logging",
        "validation",
        "skew_detection",
    )
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a mapping.")
    settings = dict(config)
    for section_name in section_names:
        section = settings.get(section_name)
        if not isinstance(section, dict):
            raise ValueError("'{}' configuration must be a mapping.".format(section_name))
        settings[section_name] = dict(section)

    generator = settings["generator"]
    record_count = (
        record_count_override
        if record_count_override is not None
        else generator.get("record_count")
    )
    if not isinstance(record_count, int) or record_count < 0:
        raise ValueError("generator.record_count must be a non-negative integer.")
    generator["record_count"] = record_count

    for key in ("customer_count", "hot_customer_count", "random_seed"):
        if not isinstance(generator.get(key), int):
            raise ValueError("generator.{} must be an integer.".format(key))
    if generator["customer_count"] < 2:
        raise ValueError("generator.customer_count must be at least 2.")
    if not 1 <= generator["hot_customer_count"] < generator["customer_count"]:
        raise ValueError(
            "generator.hot_customer_count must be between 1 and customer_count - 1."
        )
    fraction = generator.get("hot_customer_fraction")
    if not isinstance(fraction, (int, float)) or not 0 <= fraction <= 1:
        raise ValueError("generator.hot_customer_fraction must be between 0 and 1.")
    if seed_override is not None and not isinstance(seed_override, int):
        raise ValueError("seed must be an integer.")

    for section_name, key in (
        ("spark", "master"),
        ("spark", "app_name"),
        ("data", "generated_dir"),
        ("data", "output_dir"),
        ("logging", "directory"),
    ):
        if not settings[section_name].get(key):
            raise ValueError("{}.{} must be configured.".format(section_name, key))
    shuffle_partitions = settings["spark"].get("shuffle_partitions")
    if not isinstance(shuffle_partitions, int) or shuffle_partitions < 1:
        raise ValueError("spark.shuffle_partitions must be a positive integer.")

    salt_buckets = settings["optimization"].get("salt_buckets")
    if not isinstance(salt_buckets, int) or salt_buckets < 2:
        raise ValueError("optimization.salt_buckets must be at least 2.")
    repeats = settings["benchmark"].get("repeats")
    if not isinstance(repeats, int) or repeats < 1:
        raise ValueError("benchmark.repeats must be at least 1.")
    top_n = settings["skew_detection"].get("top_n")
    if not isinstance(top_n, int) or top_n < 1:
        raise ValueError("skew_detection.top_n must be at least 1.")
    currencies = settings["validation"].get("supported_currencies")
    if (
        not isinstance(currencies, list)
        or not currencies
        or any(not isinstance(currency, str) or not currency.strip() for currency in currencies)
    ):
        raise ValueError("validation.supported_currencies must be a non-empty list.")
    return settings


def _resolve_path(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _create_logger(log_path: Path, level_name: str) -> logging.Logger:
    logger = logging.getLogger("spark_skew_optimization")
    level = getattr(logging, level_name.upper(), None)
    if not isinstance(level, int):
        raise ValueError("Unsupported logging level: {}".format(level_name))
    logger.setLevel(level)
    logger.propagate = False
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def _finish_metrics(metrics: Dict[str, Any], started_clock: float) -> None:
    metrics["ended_at"] = _utc_now()
    metrics["duration_seconds"] = round(time.perf_counter() - started_clock, 3)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
