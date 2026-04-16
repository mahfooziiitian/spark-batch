"""Feature engineering pipeline — Spark aggregations → pandas for analysis.

Real-world pattern: use Spark for large-scale joins and aggregations, then
convert the compact result to pandas for feature inspection, correlation
analysis, and ML model input.
"""

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spp.session import create_spark_session


def _create_sample_data(spark: SparkSession):
    """Build sample orders and users tables."""
    rng = np.random.default_rng(42)
    n_orders = 200

    orders_pdf = pd.DataFrame(
        {
            "order_id": range(1, n_orders + 1),
            "user_id": rng.integers(1, 21, n_orders),
            "amount": np.round(rng.uniform(10, 500, n_orders), 2),
            "ts": pd.date_range("2024-01-01", periods=n_orders, freq="4h"),
        }
    )

    users_pdf = pd.DataFrame(
        {
            "user_id": range(1, 21),
            "tier": rng.choice(["free", "pro", "enterprise"], 20),
            "signup_date": pd.date_range("2023-06-01", periods=20, freq="12D"),
        }
    )

    orders = spark.createDataFrame(orders_pdf)
    users = spark.createDataFrame(users_pdf)
    return orders, users


def main(spark: SparkSession) -> None:
    orders, users = _create_sample_data(spark)

    # --- Step 1: Feature aggregation in Spark ---
    w = Window.partitionBy("user_id").orderBy("ts")

    order_features = orders.withColumn("order_rank", F.row_number().over(w))
    order_features = order_features.withColumn(
        "running_total", F.sum("amount").over(w.rowsBetween(Window.unboundedPreceding, 0))
    )

    user_features = order_features.groupBy("user_id").agg(
        F.count("order_id").alias("order_count"),
        F.round(F.sum("amount"), 2).alias("total_spend"),
        F.round(F.avg("amount"), 2).alias("avg_order_value"),
        F.round(F.stddev("amount"), 2).alias("stddev_order_value"),
        F.max("running_total").alias("lifetime_value"),
        F.datediff(F.max("ts"), F.min("ts")).alias("active_days"),
    )

    # --- Step 2: Join with user metadata ---
    features = user_features.join(users, on="user_id", how="left")

    print("=== Feature DataFrame (Spark) ===")
    features.orderBy("user_id").show(10, truncate=False)

    # --- Step 3: Pull to pandas for analysis ---
    pdf = features.toPandas()

    print("=== Feature Statistics (pandas) ===")
    print(pdf[["order_count", "total_spend", "avg_order_value"]].describe())

    print("\n=== Spend by Tier ===")
    tier_summary = pdf.groupby("tier")[["total_spend", "order_count"]].mean().round(2)
    print(tier_summary)

    print("\n=== Correlation Matrix ===")
    numeric_cols = ["order_count", "total_spend", "avg_order_value", "active_days"]
    print(pdf[numeric_cols].corr().round(3))


if __name__ == "__main__":
    spark = create_spark_session("feature-engineering")
    main(spark)
    spark.stop()
