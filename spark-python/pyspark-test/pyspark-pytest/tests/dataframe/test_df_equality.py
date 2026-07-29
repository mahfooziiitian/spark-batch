"""Tests for DataFrame equality assertions using pyspark.testing."""

import pytest
from pyspark.testing.utils import assertDataFrameEqual


class TestDataFrameEquality:
    """Tests demonstrating assertDataFrameEqual."""

    def test_equal_dataframes(self, spark):
        """Identical DataFrames pass equality assertion."""
        df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
        df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
        assertDataFrameEqual(df1, df2)

    def test_single_row_equality(self, spark):
        """Single-row DataFrames pass equality check."""
        df1 = spark.createDataFrame([("a", 1)], schema=["key", "value"])
        df2 = spark.createDataFrame([("a", 1)], schema=["key", "value"])
        assertDataFrameEqual(df1, df2)

    def test_empty_dataframes_equal(self, spark):
        """Empty DataFrames with same schema are equal."""
        df1 = spark.createDataFrame([], schema="id STRING, amount INT")
        df2 = spark.createDataFrame([], schema="id STRING, amount INT")
        assertDataFrameEqual(df1, df2)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
