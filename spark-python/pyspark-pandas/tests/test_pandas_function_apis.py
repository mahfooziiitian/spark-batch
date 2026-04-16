"""Tests for pandas function APIs — mapInPandas, applyInPandas, cogroup."""

import pytest
from pyspark.sql import functions as F

from spp.pandas_udf.map_in_pandas import (
    add_bmi,
    add_double_age,
    clean_text,
    filter_positive,
)
from spp.integration.pandas_function_apis import (
    combine_groups,
    mean_with_key,
    normalize_group,
)
from spp.integration.group_feature_engineering import (
    CATEGORY_SCHEMA,
    FEATURE_SCHEMA,
    PCT_SCHEMA,
    categorise_values,
    engineer_features,
    pct_change_features,
)
from spp.integration.ml_preprocessing import (
    FILL_SCHEMA,
    OUTLIER_SCHEMA,
    PREPROCESS_SCHEMA,
    SCALE_SCHEMA,
    fill_missing,
    full_preprocess,
    min_max_scale,
    remove_outliers,
)


class TestMapInPandas:
    def test_add_double_age(self, spark):
        df = spark.createDataFrame(
            [(1, 21), (2, 30), (3, 25)], ["id", "age"]
        )
        result = df.mapInPandas(
            add_double_age, schema="id: bigint, age: bigint, double_age: bigint"
        )
        assert result.count() == 3
        assert set(result.columns) == {"id", "age", "double_age"}
        row = result.filter(F.col("id") == 1).first()
        assert row["double_age"] == 42

    def test_add_bmi(self, spark):
        df = spark.createDataFrame(
            [(1, 70.0, 1.75), (2, 85.0, 1.80)],
            ["id", "weight_kg", "height_m"],
        )
        result = df.mapInPandas(
            add_bmi,
            schema="id: bigint, weight_kg: double, height_m: double, bmi: double",
        )
        assert result.count() == 2
        assert "bmi" in result.columns
        row = result.filter(F.col("id") == 1).first()
        assert row["bmi"] == pytest.approx(22.9, abs=0.1)

    def test_clean_text(self, spark):
        df = spark.createDataFrame(
            [(1, "  Hello WORLD  "), (2, " PySpark  ")], ["id", "text"]
        )
        result = df.mapInPandas(
            clean_text, schema="id: bigint, text: string, text_clean: string"
        )
        assert result.count() == 2
        row = result.filter(F.col("id") == 1).first()
        assert row["text_clean"] == "hello world"

    def test_filter_positive(self, spark):
        df = spark.createDataFrame(
            [(1, -5.0), (2, 3.0), (3, -1.0), (4, 7.0)], ["id", "value"]
        )
        result = df.mapInPandas(
            filter_positive, schema="id: bigint, value: double"
        )
        assert result.count() == 2
        assert result.filter(F.col("value") <= 0).count() == 0


class TestApplyInPandas:
    def test_normalize_group(self, spark):
        df = spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 10.0)],
            ["group_id", "value"],
        )
        result = df.groupBy("group_id").applyInPandas(
            normalize_group,
            schema="group_id: long, value: double, normalized: double",
        )
        assert result.count() == 6
        assert "normalized" in result.columns

    def test_mean_with_key(self, spark):
        df = spark.createDataFrame(
            [(1, 10.0), (1, 20.0), (2, 30.0)], ["group_id", "value"]
        )
        result = df.groupBy("group_id").applyInPandas(
            mean_with_key, schema="group_id: long, mean_value: double"
        )
        assert result.count() == 2
        row = result.filter(F.col("group_id") == 1).first()
        assert row["mean_value"] == pytest.approx(15.0)

    def test_engineer_features(self, spark):
        df = spark.createDataFrame(
            [
                (1, 1, 10.0), (1, 2, 20.0), (1, 3, 30.0),
                (2, 1, 5.0), (2, 2, 15.0),
            ],
            ["entity_id", "ts", "value"],
        )
        result = df.groupBy("entity_id").applyInPandas(
            engineer_features, schema=FEATURE_SCHEMA
        )
        assert result.count() == 5
        assert set(result.columns) == {
            "entity_id", "ts", "value", "rolling_mean_3", "lag_1", "cumsum"
        }

    def test_categorise_values(self, spark):
        df = spark.createDataFrame(
            [(1, 1, 5.0), (1, 2, 25.0), (1, 3, 80.0)],
            ["entity_id", "ts", "value"],
        )
        result = df.groupBy("entity_id").applyInPandas(
            categorise_values, schema=CATEGORY_SCHEMA
        )
        assert result.count() == 3
        assert "category" in result.columns
        categories = {r["category"] for r in result.collect()}
        assert categories == {"low", "medium", "high"}

    def test_pct_change_features(self, spark):
        df = spark.createDataFrame(
            [(1, 1, 10.0), (1, 2, 20.0), (1, 3, 30.0)],
            ["entity_id", "ts", "value"],
        )
        result = df.groupBy("entity_id").applyInPandas(
            pct_change_features, schema=PCT_SCHEMA
        )
        assert result.count() == 3
        assert "pct_change" in result.columns
        # First row pct_change should be 0.0 (fillna)
        first = result.filter(F.col("ts") == 1).first()
        assert first["pct_change"] == pytest.approx(0.0)


class TestCogroupApplyInPandas:
    def test_combine_groups(self, spark):
        df1 = spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0)], ["id", "value1"]
        )
        df2 = spark.createDataFrame(
            [(1, "A"), (2, "B"), (2, "C")], ["id", "value2"]
        )
        result = (
            df1.groupBy("id")
            .cogroup(df2.groupBy("id"))
            .applyInPandas(
                combine_groups,
                schema="id: long, value1: double, value2: string",
            )
        )
        assert result.count() >= 3
        assert set(result.columns) == {"id", "value1", "value2"}

    def test_combine_groups_no_match(self, spark):
        """Left merge keeps rows from df1 even when df2 has no match."""
        df1 = spark.createDataFrame([(1, 10.0), (3, 30.0)], ["id", "value1"])
        df2 = spark.createDataFrame([(1, "X")], ["id", "value2"])
        result = (
            df1.groupBy("id")
            .cogroup(df2.groupBy("id"))
            .applyInPandas(
                combine_groups,
                schema="id: long, value1: double, value2: string",
            )
        )
        assert result.count() == 2
        unmatched = result.filter(F.col("id") == 3).first()
        assert unmatched["value2"] is None


class TestMLPreprocessing:
    def test_fill_missing(self, spark):
        df = spark.createDataFrame(
            [
                (1, 10.0, 1.0),
                (1, None, 2.0),
                (1, 20.0, None),
            ],
            ["group_id", "feature_a", "feature_b"],
        )
        result = df.groupBy("group_id").applyInPandas(
            fill_missing, schema=FILL_SCHEMA
        )
        assert result.count() == 3
        # No nulls should remain in feature_a or feature_b
        assert result.filter(F.col("feature_a").isNull()).count() == 0
        assert result.filter(F.col("feature_b").isNull()).count() == 0

    def test_remove_outliers(self, spark):
        df = spark.createDataFrame(
            [
                (1, 10.0, 1.0),
                (1, 12.0, 2.0),
                (1, 11.0, 3.0),
                (1, 10.5, 4.0),
                (1, 11.5, 5.0),
                (1, 500.0, 6.0),  # extreme outlier
            ],
            ["group_id", "feature_a", "feature_b"],
        )
        result = df.groupBy("group_id").applyInPandas(
            remove_outliers, schema=OUTLIER_SCHEMA
        )
        assert result.count() < 6
        assert result.filter(F.col("feature_a") == 500.0).count() == 0

    def test_min_max_scale(self, spark):
        df = spark.createDataFrame(
            [
                (1, 10.0, 1.0),
                (1, 20.0, 2.0),
                (1, 30.0, 3.0),
            ],
            ["group_id", "feature_a", "feature_b"],
        )
        result = df.groupBy("group_id").applyInPandas(
            min_max_scale, schema=SCALE_SCHEMA
        )
        assert result.count() == 3
        assert "scaled_a" in result.columns
        # All scaled values should be in [0, 1]
        assert result.filter(F.col("scaled_a") < 0).count() == 0
        assert result.filter(F.col("scaled_a") > 1).count() == 0
        # Min should map to 0, max to 1
        row_min = result.filter(F.col("feature_a") == 10.0).first()
        row_max = result.filter(F.col("feature_a") == 30.0).first()
        assert row_min["scaled_a"] == pytest.approx(0.0)
        assert row_max["scaled_a"] == pytest.approx(1.0)

    def test_full_preprocess(self, spark):
        df = spark.createDataFrame(
            [
                (1, 10.0, 1.0),
                (1, 20.0, 2.0),
                (1, None, 3.0),
                (1, 15.0, None),
                (1, 100.0, 5.0),  # potential outlier
            ],
            ["group_id", "feature_a", "feature_b"],
        )
        result = df.groupBy("group_id").applyInPandas(
            full_preprocess, schema=PREPROCESS_SCHEMA
        )
        # Should have fewer rows (outlier removed) and no nulls
        assert result.count() <= 5
        assert result.filter(F.col("feature_a").isNull()).count() == 0
        assert "scaled_a" in result.columns
        # Scaled values in [0, 1]
        assert result.filter(F.col("scaled_a") < 0).count() == 0
        assert result.filter(F.col("scaled_a") > 1).count() == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
