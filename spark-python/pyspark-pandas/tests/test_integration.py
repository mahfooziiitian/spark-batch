"""Integration tests for pandas ↔ Spark patterns."""

import numpy as np
import pandas as pd
import pyspark.pandas as ps
import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType


class TestConversionPatterns:
    def test_spark_to_pandas_round_trip(self, spark):
        """Spark → pandas → Spark preserves data."""
        df = spark.createDataFrame(
            [(1, "alice", 100.0), (2, "bob", 200.0)],
            ["id", "name", "revenue"],
        )
        pdf = df.toPandas()
        assert len(pdf) == 2
        assert list(pdf.columns) == ["id", "name", "revenue"]

        df_back = spark.createDataFrame(pdf)
        assert df_back.count() == 2
        assert set(df_back.columns) == {"id", "name", "revenue"}

    def test_pandas_api_conversion(self, spark):
        """pandas_api() and to_spark() round-trip."""
        df = spark.createDataFrame(
            [(1, 10.0), (2, 20.0), (3, 30.0)], ["id", "value"]
        )
        psdf = df.pandas_api()
        assert len(psdf) == 3

        sdf = psdf.to_spark()
        assert sdf.count() == 3
        assert set(sdf.columns) == {"id", "value"}

    def test_arrow_create_dataframe(self, spark):
        """Arrow-based createDataFrame from a pandas DataFrame."""
        pdf = pd.DataFrame(
            {"user_id": range(1, 6), "score": [10.0, 20.0, 30.0, 40.0, 50.0]}
        )
        df = spark.createDataFrame(pdf)
        assert df.count() == 5
        assert set(df.columns) == {"user_id", "score"}
        row = df.filter(F.col("user_id") == 3).first()
        assert row["score"] == pytest.approx(30.0)


class TestFeatureEngineering:
    def test_aggregation_to_pandas(self, spark):
        """Spark aggregation → toPandas produces correct feature matrix."""
        orders = spark.createDataFrame(
            [
                (1, 1, 100.0),
                (2, 1, 200.0),
                (3, 2, 150.0),
                (4, 2, 50.0),
                (5, 3, 300.0),
            ],
            ["order_id", "user_id", "amount"],
        )
        features = orders.groupBy("user_id").agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("amount"), 2).alias("total_spend"),
            F.round(F.avg("amount"), 2).alias("avg_order_value"),
        )
        assert features.count() == 3
        assert set(features.columns) == {
            "user_id", "order_count", "total_spend", "avg_order_value"
        }

        pdf = features.toPandas()
        assert len(pdf) == 3
        user1 = pdf[pdf["user_id"] == 1].iloc[0]
        assert user1["order_count"] == 2
        assert user1["total_spend"] == pytest.approx(300.0)


class TestMLPipeline:
    def test_pandas_udf_scoring(self, spark):
        """Pandas UDF applies vectorized scoring across a Spark DataFrame."""
        df = spark.createDataFrame(
            [(1, 2.0), (2, 4.0), (3, 6.0), (4, 8.0), (5, 10.0)],
            ["id", "feature"],
        )

        @pandas_udf(DoubleType())
        def score(feature: pd.Series) -> pd.Series:
            return feature * 2.5 + 1.0

        result = df.withColumn("prediction", score("feature"))
        assert result.count() == 5
        assert "prediction" in result.columns
        row = result.filter(F.col("id") == 1).first()
        assert row["prediction"] == pytest.approx(6.0)

    def test_feature_engineering_and_scoring(self, spark):
        """End-to-end: Spark feature prep → pandas UDF scoring."""
        raw = spark.createDataFrame(
            [(1, 10.0, "a"), (2, 20.0, "b"), (3, 30.0, "a")],
            ["id", "value", "cat"],
        )
        features = raw.withColumn(
            "value_log", F.log1p("value")
        ).withColumn(
            "cat_encoded",
            F.when(F.col("cat") == "a", 0).otherwise(1),
        )

        @pandas_udf(DoubleType())
        def predict(val: pd.Series, cat: pd.Series) -> pd.Series:
            return val * 0.5 + cat * 10.0

        scored = features.withColumn(
            "prediction", predict("value_log", "cat_encoded")
        )
        assert scored.count() == 3
        assert set(scored.columns) == {
            "id", "value", "cat", "value_log", "cat_encoded", "prediction"
        }


class TestHybridWorkflow:
    def test_etl_sample_analysis(self, spark):
        """ETL → sample → analysis pattern works end to end."""
        rng = np.random.default_rng(42)
        n = 100
        pdf = pd.DataFrame(
            {
                "event_id": range(1, n + 1),
                "user_id": rng.integers(1, 11, n),
                "event_type": rng.choice(["click", "view", "purchase"], n),
                "amount": np.round(rng.uniform(5, 200, n), 2),
            }
        )
        raw = spark.createDataFrame(pdf)

        # ETL step: clean and aggregate
        cleaned = raw.withColumn(
            "amount", F.coalesce(F.col("amount"), F.lit(0.0))
        )
        summary = cleaned.groupBy("event_type").agg(
            F.count("event_id").alias("event_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        assert summary.count() == 3
        assert set(summary.columns) == {"event_type", "event_count", "total_amount"}

        # Sample step: pull to pandas
        sample_pdf = cleaned.sample(fraction=0.5, seed=42).toPandas()
        assert len(sample_pdf) > 0
        assert "amount" in sample_pdf.columns

        # Analysis step: pandas aggregation on sample
        type_counts = sample_pdf["event_type"].value_counts()
        assert len(type_counts) > 0

    def test_pandas_udf_transform(self, spark):
        """Custom transform with pandas UDF in a hybrid workflow."""
        df = spark.createDataFrame(
            [(1, 10.0), (2, 50.0), (3, 100.0), (4, 200.0)],
            ["id", "amount"],
        )

        @pandas_udf("string")
        def classify(amount: pd.Series) -> pd.Series:
            bins = [0, 25, 75, float("inf")]
            labels = ["small", "medium", "large"]
            return pd.cut(amount, bins=bins, labels=labels).astype(str)

        result = df.withColumn("tier", classify("amount"))
        assert result.count() == 4
        assert "tier" in result.columns
        tiers = {r["tier"] for r in result.collect()}
        assert tiers == {"small", "medium", "large"}


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
