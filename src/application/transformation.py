"""Transaction transformations and aggregations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_by_customer(transactions: DataFrame) -> DataFrame:
    return transactions.groupBy("customer_id").agg(
        F.count(F.lit(1)).cast("long").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
    )
