"""Tests for PyDeequ ColumnProfilerRunner."""

import pytest
from pydeequ.profiles import ColumnProfilerRunner
from pyspark.sql import Row


class TestProfiling:
    """Tests for column-level data profiling."""

    def test_profiler_returns_all_columns(self, spark):
        """Profiler produces a profile for each column in the DataFrame."""
        df = spark.createDataFrame([Row(a="foo", b=1, c=5.0), Row(a="bar", b=2, c=6.0)])
        result = ColumnProfilerRunner(spark).onData(df).run()
        assert set(result.profiles.keys()) == {"a", "b", "c"}

    def test_profile_completeness(self, spark):
        """Profile reports correct completeness for columns with nulls."""
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a=None, b=2), Row(a="baz", b=None)])
        result = ColumnProfilerRunner(spark).onData(df).run()
        a_profile = result.profiles["a"]
        assert abs(a_profile.completeness - 2.0 / 3.0) < 1e-6

    def test_profile_single_column(self, spark):
        """Profiler works on a single-column DataFrame."""
        df = spark.createDataFrame([Row(x=1), Row(x=2), Row(x=3)])
        result = ColumnProfilerRunner(spark).onData(df).run()
        assert "x" in result.profiles


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
