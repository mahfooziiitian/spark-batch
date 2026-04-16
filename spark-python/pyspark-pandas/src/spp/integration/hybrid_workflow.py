"""Hybrid workflow — batch ETL in Spark + sampling and debugging in pandas.

Real-world pattern: use Spark for full-scale ETL, then switch to pandas
for sampling, data-quality debugging, and statistical summaries.
"""

import os

import numpy as np
import pandas as pd
import spp._env  # noqa: F401
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

from spp.session import create_spark_session

OUTPUT_PATH = os.environ.get("OUTPUT_PATH", "/tmp/hybrid_workflow")


def _generate_raw_data(spark: SparkSession):
    """Simulate a raw events table."""
    rng = np.random.default_rng(42)
    n = 1_000

    pdf = pd.DataFrame(
        {
            "event_id": range(1, n + 1),
            "user_id": rng.integers(1, 51, n),
            "event_type": rng.choice(["click", "view", "purchase", "signup"], n),
            "amount": np.where(
                rng.random(n) > 0.6,
                np.round(rng.uniform(5, 200, n), 2),
                np.nan,
            ),
            "ts": pd.date_range("2024-01-01", periods=n, freq="15min"),
        }
    )
    return spark.createDataFrame(pdf)


def step_etl(spark: SparkSession):
    """Step 1: Batch ETL — clean, enrich, aggregate in Spark."""
    print("=" * 60)
    print("Step 1: Batch ETL (Spark)")
    print("=" * 60)

    raw = _generate_raw_data(spark)

    cleaned = (
        raw
        .withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
        .withColumn("event_date", F.to_date("ts"))
        .withColumn("event_hour", F.hour("ts"))
    )

    daily_summary = cleaned.groupBy("event_date", "event_type").agg(
        F.count("event_id").alias("event_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.countDistinct("user_id").alias("unique_users"),
    )

    daily_summary.orderBy("event_date", "event_type").show(10)
    daily_summary.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/daily_summary")
    print(f"Written to {OUTPUT_PATH}/daily_summary\n")
    return cleaned


def step_sample_debug(spark: SparkSession, cleaned):
    """Step 2: Sample and debug in pandas."""
    print("=" * 60)
    print("Step 2: Sampling & Debugging (pandas)")
    print("=" * 60)

    # Pull a 10% sample to pandas for inspection
    sample_df = cleaned.sample(fraction=0.1, seed=42)
    sample_pdf = sample_df.toPandas()

    print(f"Sample size: {len(sample_pdf)} rows")
    print("\n--- Data types ---")
    print(sample_pdf.dtypes)

    print("\n--- Null check ---")
    print(sample_pdf.isnull().sum())

    print("\n--- Amount distribution ---")
    print(sample_pdf["amount"].describe())

    print("\n--- Event type breakdown ---")
    print(sample_pdf["event_type"].value_counts())


def step_pandas_api_analysis():
    """Step 3: Full-scale analysis using Pandas API on Spark."""
    print("\n" + "=" * 60)
    print("Step 3: Large-scale Analysis (Pandas API on Spark)")
    print("=" * 60)

    psdf = ps.read_parquet(f"{OUTPUT_PATH}/daily_summary", index_col=None)
    print("\nColumns:", list(psdf.columns))

    print("\n--- Summary stats ---")
    print(psdf[["event_count", "total_amount", "unique_users"]].describe())

    print("\n--- Top event types by total amount ---")
    print(psdf.groupby("event_type")["total_amount"].sum().sort_values(ascending=False))


def step_custom_transform(spark: SparkSession, cleaned):
    """Step 4: Complex transformation using Pandas UDF."""
    print("\n" + "=" * 60)
    print("Step 4: Custom Transform (Pandas UDF)")
    print("=" * 60)

    @pandas_udf(StringType())
    def classify_amount(amount: pd.Series) -> pd.Series:
        bins = [0, 25, 75, 150, float("inf")]
        labels = ["micro", "small", "medium", "large"]
        return pd.cut(amount, bins=bins, labels=labels).astype(str)

    result = (
        cleaned
        .filter(F.col("amount") > 0)
        .withColumn("amount_tier", classify_amount("amount"))
    )

    result.groupBy("amount_tier").count().orderBy("count", ascending=False).show()


def main(spark: SparkSession) -> None:
    cleaned = step_etl(spark)
    step_sample_debug(spark, cleaned)
    step_pandas_api_analysis()
    step_custom_transform(spark, cleaned)


if __name__ == "__main__":
    spark = create_spark_session("hybrid-workflow")
    main(spark)
    spark.stop()
