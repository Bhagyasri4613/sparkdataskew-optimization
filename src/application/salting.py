"""Salted aggregation optimization."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_by_customer_with_salting(
    transactions: DataFrame, salt_buckets: int
) -> DataFrame:
    if salt_buckets < 2:
        raise ValueError("salt_buckets must be at least 2.")

    salted = transactions.withColumn(
        "_salt",
        F.pmod(F.xxhash64(F.col("transaction_id")), F.lit(salt_buckets)),
    )
    partial = salted.groupBy("customer_id", "_salt").agg(
        F.count(F.lit(1)).cast("long").alias("partial_count"),
        F.sum("amount").alias("partial_amount"),
    )
    return partial.groupBy("customer_id").agg(
        F.sum("partial_count").cast("long").alias("transaction_count"),
        F.sum("partial_amount").alias("total_amount"),
    )
