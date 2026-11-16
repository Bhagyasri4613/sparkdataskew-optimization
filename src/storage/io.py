"""Helpers for reading and writing project data."""

import csv
import json
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import DataFrame


def write_frame(frame: DataFrame, path: Path, output_format: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_format = output_format.lower()
    if normalized_format not in ("json", "csv"):
        raise ValueError(
            "Unsupported output format '{}'; use JSON or CSV.".format(output_format)
        )

    with path.open("w", encoding="utf-8", newline="") as output_file:
        if normalized_format == "csv":
            writer = csv.DictWriter(output_file, fieldnames=frame.columns)
            writer.writeheader()
            for row in frame.toLocalIterator():
                writer.writerow(
                    {
                        column: _format_value(row[column])
                        for column in frame.columns
                    }
                )
        else:
            for row in frame.toLocalIterator():
                output_file.write(
                    json.dumps(row.asDict(recursive=True), default=str) + "\n"
                )


def _format_value(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def write_json_report(path: Path, report: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, sort_keys=True, default=str)
        report_file.write("\n")
