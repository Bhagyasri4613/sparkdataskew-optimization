import csv
from pathlib import Path

from src.application.data_generator import write_synthetic_transactions


def test_generator_is_reproducible_and_skewed(tmp_path: Path):
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    settings = {
        "record_count": 5000,
        "seed": 71,
        "customer_count": 100,
        "hot_customer_count": 3,
        "hot_customer_fraction": 0.8,
    }

    write_synthetic_transactions(path=first, **settings)
    write_synthetic_transactions(path=second, **settings)

    assert first.read_bytes() == second.read_bytes()
    with first.open(encoding="utf-8", newline="") as input_file:
        counts = {}
        for row in csv.DictReader(input_file):
            customer_id = row["customer_id"]
            counts[customer_id] = counts.get(customer_id, 0) + 1
    hot_count = sum(counts.get("customer-{:06d}".format(i), 0) for i in range(1, 4))
    assert hot_count / settings["record_count"] > 0.75


def test_generator_supports_at_least_one_hundred_thousand_records(tmp_path: Path):
    output = tmp_path / "large.csv"
    write_synthetic_transactions(
        path=output,
        record_count=100000,
        seed=42,
        customer_count=1000,
        hot_customer_count=10,
        hot_customer_fraction=0.8,
    )

    with output.open(encoding="utf-8", newline="") as input_file:
        row_count = sum(1 for _ in csv.DictReader(input_file))

    assert row_count == 100000
