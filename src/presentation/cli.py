"""Presentation-layer command-line interface."""

import argparse
from pathlib import Path
from typing import Any, Dict

from src.application.pipeline import run_pipeline
from src.config import load_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and benchmark a skewed Spark transaction workload"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.yaml"),
        help="Path to YAML configuration (default: config.yaml)",
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Optional CSV, JSON/JSONL, or Parquet input; generate data if omitted",
    )
    parser.add_argument(
        "--records",
        type=int,
        help="Override the configured synthetic record count",
    )
    parser.add_argument("--seed", type=int, help="Override the generator seed")
    parser.add_argument(
        "--run-id",
        help="Optional run ID for logs and metrics; generated automatically by default",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    metrics = run_pipeline(
        config=config,
        project_root=args.config.resolve().parent,
        input_path=args.input,
        record_count=args.records,
        seed=args.seed,
        run_id=args.run_id,
    )
    print_summary(metrics)


def print_summary(metrics: Dict[str, Any]) -> None:
    print("Run ID: {}".format(metrics["run_id"]))
    print("Status: {}".format(metrics["status"]))
    print(
        "Input: {input_count}; accepted: {accepted_count}; rejected: {rejected_count}".format(
            **metrics
        )
    )
    print(
        "Aggregation groups: {output_count}; results match: {results_match}".format(
            **metrics
        )
    )
    print(
        "Median seconds - normal: {normal_median_seconds:.4f}, salted: "
        "{salted_median_seconds:.4f}".format(**metrics)
    )
