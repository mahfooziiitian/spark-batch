"""Tests for pandas-on-Spark operations."""

import numpy as np
import pyspark.pandas as ps
import pytest


class TestPandasOnSparkDataFrame:
    def test_head(self, spark):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5]})
        assert len(psdf.head(3)) == 3

    def test_describe(self, spark):
        psdf = ps.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        desc = psdf.describe()
        assert "mean" in desc.index.to_list()

    def test_sort_values(self, spark):
        psdf = ps.DataFrame({"a": [3, 1, 2]})
        sorted_df = psdf.sort_values("a")
        assert sorted_df["a"].to_list() == [1, 2, 3]

    def test_column_selection(self, spark):
        psdf = ps.DataFrame({"a": [1], "b": [2], "c": [3]})
        subset = psdf[["a", "c"]]
        assert set(subset.columns) == {"a", "c"}

    def test_boolean_filter(self, spark):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5]})
        filtered = psdf[psdf["a"] > 3]
        assert len(filtered) == 2


class TestMissingData:
    def test_dropna(self, spark):
        psdf = ps.DataFrame({"a": [1.0, np.nan, 3.0], "b": [4.0, 5.0, np.nan]})
        result = psdf.dropna()
        assert len(result) == 1

    def test_fillna(self, spark):
        psdf = ps.DataFrame({"a": [1.0, np.nan, 3.0]})
        result = psdf.fillna(0)
        assert result["a"].to_list() == [1.0, 0.0, 3.0]

    def test_isna_count(self, spark):
        psdf = ps.DataFrame({"a": [1.0, np.nan, np.nan]})
        assert psdf["a"].isna().sum() == 2


class TestGroupByAgg:
    def test_groupby_sum(self, spark):
        psdf = ps.DataFrame({"grp": ["a", "a", "b"], "val": [10, 20, 30]})
        result = psdf.groupby("grp")["val"].sum()
        assert result.loc["a"] == 30
        assert result.loc["b"] == 30

    def test_value_counts(self, spark):
        psdf = ps.DataFrame({"city": ["NYC", "LA", "NYC", "NYC"]})
        counts = psdf["city"].value_counts()
        assert counts.loc["NYC"] == 3
        assert counts.loc["LA"] == 1


class TestStringOps:
    def test_upper(self, spark):
        psdf = ps.DataFrame({"name": ["alice", "bob"]})
        result = psdf["name"].str.upper().to_list()
        assert result == ["ALICE", "BOB"]

    def test_contains(self, spark):
        psdf = ps.DataFrame({"email": ["a@test.com", "b@work.org"]})
        filtered = psdf[psdf["email"].str.contains("test")]
        assert len(filtered) == 1

    def test_split(self, spark):
        from pyspark.sql import functions as F

        psdf = ps.DataFrame({"name": ["Alice Smith", "Bob Jones"]})
        sdf = psdf.to_spark(index_col="__idx__")
        parts = F.split(F.col("name"), " ")
        sdf = sdf.withColumn("first", parts.getItem(0))
        result = sdf.select("first").toPandas()["first"].tolist()
        assert result == ["Alice", "Bob"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
