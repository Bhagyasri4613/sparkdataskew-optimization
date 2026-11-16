"""Runtime and correctness benchmarking."""

from decimal import Decimal
from statistics import median
from time import perf_counter
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame

from src.application.salting import aggregate_by_customer_with_salting
from src.application.transformation import aggregate_by_customer


def benchmark_aggregations(
    transactions: DataFrame, salt_buckets: int, repeats: int
) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
    if repeats < 1:
        raise ValueError("repeats must be at least 1.")

    normal = aggregate_by_customer(transactions)
    salted = aggregate_by_customer_with_salting(transactions, salt_buckets)
    timings: Dict[str, List[float]] = {"normal": [], "salted": []}
    normal_rows = []
    salted_rows = []

    for _ in range(repeats):
        for name, frame in (("normal", normal), ("salted", salted)):
            started = perf_counter()
            result = frame.orderBy("customer_id").collect()
            timings[name].append(perf_counter() - started)
            if name == "normal":
                normal_rows = result
            else:
                salted_rows = result

    results_match = _canonical_rows(normal_rows) == _canonical_rows(salted_rows)
    normal_median = median(timings["normal"])
    salted_median = median(timings["salted"])
    return normal, salted, {
        "repeats": repeats,
        "normal_seconds": timings["normal"],
        "salted_seconds": timings["salted"],
        "normal_median_seconds": normal_median,
        "salted_median_seconds": salted_median,
        "speedup_ratio": (
            normal_median / salted_median if salted_median > 0 else None
        ),
        "output_count": len(normal_rows),
        "results_match": results_match,
    }


def _canonical_rows(rows: list) -> List[Tuple[str, int, Decimal]]:
    return [
        (
            row["customer_id"],
            int(row["transaction_count"]),
            Decimal(row["total_amount"]),
        )
        for row in rows
    ]
