"""Customer-key skew measurement."""

from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def detect_customer_skew(transactions: DataFrame, top_n: int = 10) -> Dict[str, Any]:
    if top_n < 1:
        raise ValueError("top_n must be at least 1.")

    counts = transactions.groupBy("customer_id").agg(
        F.count(F.lit(1)).alias("transaction_count")
    )
    rows = counts.orderBy(F.desc("transaction_count"), F.asc("customer_id")).collect()
    total = sum(row["transaction_count"] for row in rows)
    distinct_customers = len(rows)
    maximum = max((row["transaction_count"] for row in rows), default=0)
    average = total / distinct_customers if distinct_customers else 0.0
    return {
        "transaction_count": total,
        "distinct_customers": distinct_customers,
        "max_customer_count": maximum,
        "max_customer_fraction": maximum / total if total else 0.0,
        "average_rows_per_customer": average,
        "max_to_average_ratio": maximum / average if average else 0.0,
        "top_customers": [
            {
                "customer_id": row["customer_id"],
                "transaction_count": row["transaction_count"],
            }
            for row in rows[:top_n]
        ],
    }
