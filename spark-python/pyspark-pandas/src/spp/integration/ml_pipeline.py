"""ML pipeline — feature prep in Spark, model training in pandas/sklearn.

Demonstrates the common pattern where Spark handles large-scale feature
preparation while pandas and scikit-learn handle model training on the
driver node.
"""

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from spp.session import create_spark_session


def _create_training_data(spark: SparkSession):
    """Generate synthetic training data in Spark."""
    rng = np.random.default_rng(42)
    n = 500

    pdf = pd.DataFrame(
        {
            "feature_a": rng.standard_normal(n),
            "feature_b": rng.uniform(0, 100, n),
            "feature_c": rng.choice(["cat", "dog", "bird"], n),
            "noise": rng.standard_normal(n) * 0.1,
        }
    )
    # Target: linear combination + noise
    pdf["target"] = 3.0 * pdf["feature_a"] + 0.5 * pdf["feature_b"] + pdf["noise"]

    return spark.createDataFrame(pdf)


def main(spark: SparkSession) -> None:
    raw_df = _create_training_data(spark)

    # --- Step 1: Feature engineering in Spark ---
    print("=== Step 1: Feature Engineering (Spark) ===")

    features_df = (
        raw_df
        .withColumn("feature_b_log", F.log1p("feature_b"))
        .withColumn("ab_interaction", F.col("feature_a") * F.col("feature_b"))
        .withColumn(
            "category_encoded",
            F.when(F.col("feature_c") == "cat", 0)
            .when(F.col("feature_c") == "dog", 1)
            .otherwise(2),
        )
        .drop("feature_c", "noise")
    )

    features_df.printSchema()
    features_df.show(5, truncate=False)

    # --- Step 2: Pull to pandas for training ---
    print("=== Step 2: Train/Test Split (pandas) ===")

    pdf = features_df.toPandas()
    feature_cols = ["feature_a", "feature_b", "feature_b_log", "ab_interaction", "category_encoded"]

    X = pdf[feature_cols].values
    y = pdf["target"].values

    # Manual train/test split (avoids sklearn import for portability)
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    print(f"Train: {X_train.shape[0]} rows, Test: {X_test.shape[0]} rows")

    # --- Step 3: Train a simple model (numpy least squares) ---
    print("\n=== Step 3: Train Model (numpy) ===")

    # Normal equation: w = (X^T X)^{-1} X^T y
    X_bias = np.column_stack([np.ones(len(X_train)), X_train])
    weights = np.linalg.lstsq(X_bias, y_train, rcond=None)[0]

    print(f"Weights: {np.round(weights, 4)}")

    X_test_bias = np.column_stack([np.ones(len(X_test)), X_test])
    predictions = X_test_bias @ weights
    rmse = np.sqrt(np.mean((y_test - predictions) ** 2))
    print(f"Test RMSE: {rmse:.4f}")

    # --- Step 4: Score full dataset back in Spark ---
    print("\n=== Step 4: Score in Spark (Pandas UDF) ===")

    broadcast_weights = spark.sparkContext.broadcast(weights)

    @pandas_udf(DoubleType())
    def predict(
        feat_a: pd.Series,
        feat_b: pd.Series,
        feat_b_log: pd.Series,
        ab_inter: pd.Series,
        cat_enc: pd.Series,
    ) -> pd.Series:
        w = broadcast_weights.value
        X = np.column_stack([np.ones(len(feat_a)), feat_a, feat_b, feat_b_log, ab_inter, cat_enc])
        return pd.Series(X @ w)

    scored = features_df.withColumn(
        "prediction",
        predict("feature_a", "feature_b", "feature_b_log", "ab_interaction", "category_encoded"),
    )
    scored.select("feature_a", "feature_b", "target", "prediction").show(10)


if __name__ == "__main__":
    spark = create_spark_session("ml-pipeline")
    main(spark)
    spark.stop()
