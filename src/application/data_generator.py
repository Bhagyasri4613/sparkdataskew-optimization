"""Reproducible synthetic transaction data generation."""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


def write_synthetic_transactions(
    path: Path,
    record_count: int,
    seed: int,
    customer_count: int,
    hot_customer_count: int,
    hot_customer_fraction: float,
) -> int:
    if record_count < 0:
        raise ValueError("record_count must be zero or greater.")
    if customer_count < 2:
        raise ValueError("customer_count must be at least 2.")
    if not 1 <= hot_customer_count < customer_count:
        raise ValueError("hot_customer_count must be between 1 and customer_count - 1.")
    if not 0 <= hot_customer_fraction <= 1:
        raise ValueError("hot_customer_fraction must be between 0 and 1.")

    path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    start_time = datetime(2025, 1, 1)
    fieldnames = [
        "transaction_id",
        "customer_id",
        "transaction_ts",
        "amount",
        "currency",
        "merchant_id",
    ]

    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for index in range(record_count):
            if rng.random() < hot_customer_fraction:
                customer_number = rng.randint(1, hot_customer_count)
            else:
                customer_number = rng.randint(hot_customer_count + 1, customer_count)

            transaction_time = start_time + timedelta(seconds=index)
            writer.writerow(
                {
                    "transaction_id": "txn-{:012d}".format(index),
                    "customer_id": "customer-{:06d}".format(customer_number),
                    "transaction_ts": transaction_time.isoformat(timespec="seconds"),
                    "amount": "{:.2f}".format(rng.randint(1, 100000) / 100),
                    "currency": "USD",
                    "merchant_id": "merchant-{:04d}".format(rng.randint(1, 1000)),
                }
            )
    return record_count
