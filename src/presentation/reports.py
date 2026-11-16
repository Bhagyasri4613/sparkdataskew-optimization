"""Presentation-layer benchmark and pipeline reports."""

import csv
from pathlib import Path
from typing import Any, Dict


def write_benchmark_report(report: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as report_file:
        writer = csv.DictWriter(
            report_file,
            fieldnames=(
                "method",
                "repeat_seconds",
                "median_seconds",
                "speedup_ratio",
                "results_match",
                "salt_buckets",
            ),
        )
        writer.writeheader()
        for method in ("normal", "salted"):
            writer.writerow(
                {
                    "method": method,
                    "repeat_seconds": ";".join(
                        "{:.6f}".format(value)
                        for value in report["{}_seconds".format(method)]
                    ),
                    "median_seconds": "{:.6f}".format(
                        report["{}_median_seconds".format(method)]
                    ),
                    "speedup_ratio": (
                        "{:.6f}".format(report["speedup_ratio"])
                        if report["speedup_ratio"] is not None
                        else ""
                    ),
                    "results_match": report["results_match"],
                    "salt_buckets": report["salt_buckets"],
                }
            )
