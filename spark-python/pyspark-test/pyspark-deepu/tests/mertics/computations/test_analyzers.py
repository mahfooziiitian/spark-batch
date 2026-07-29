"""Tests for PyDeequ AnalysisRunner and analyzer metrics."""

import pytest
from pydeequ.analyzers import (
    AnalysisRunner,
    AnalyzerContext,
    Completeness,
    Size,
)
from pyspark.sql import Row
from pyspark.sql import functions as F


class TestAnalyzers:
    """Tests for PyDeequ analyzer computations."""

    def test_size_analyzer(self, spark):
        """Size analyzer returns correct row count."""
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a="bar", b=2), Row(a="baz", b=3)])
        result = AnalysisRunner(spark).onData(df).addAnalyzer(Size()).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(spark, result)

        size_row = result_df.filter(F.col("name") == "Size").first()
        assert size_row is not None
        assert size_row["value"] == 3.0

    def test_completeness_all_present(self, spark):
        """Completeness is 1.0 when all values are non-null."""
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a="bar", b=2)])
        result = AnalysisRunner(spark).onData(df).addAnalyzer(Completeness("b")).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(spark, result)

        row = result_df.filter(F.col("name") == "Completeness").first()
        assert row is not None
        assert row["value"] == 1.0

    def test_completeness_with_nulls(self, spark):
        """Completeness reflects the fraction of non-null values."""
        df = spark.createDataFrame(
            [Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]
        )
        result = AnalysisRunner(spark).onData(df).addAnalyzer(Completeness("c")).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(spark, result)

        row = result_df.filter(F.col("name") == "Completeness").first()
        assert row is not None
        assert abs(row["value"] - 2.0 / 3.0) < 1e-6

    def test_multiple_analyzers(self, spark):
        """Multiple analyzers produce multiple result rows."""
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a="bar", b=2)])
        result = (
            AnalysisRunner(spark)
            .onData(df)
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("a"))
            .addAnalyzer(Completeness("b"))
            .run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(spark, result)
        assert result_df.count() >= 3

    def test_single_row_dataframe(self, spark):
        """Analyzers work on single-row DataFrames."""
        df = spark.createDataFrame([Row(a="only", b=42)])
        result = AnalysisRunner(spark).onData(df).addAnalyzer(Size()).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(spark, result)

        row = result_df.filter(F.col("name") == "Size").first()
        assert row["value"] == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
