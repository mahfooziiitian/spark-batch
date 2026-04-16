"""Tests for PyDeequ analyzer computations."""

import pytest
from pyspark.sql import functions as F

from dpu.mertics.computations.analyzers.analyzers import run_basic_analysis, run_retail_analysis
from dpu.sample_data import create_retail_df, create_sample_df


class TestAnalyzers:
    """Tests for PyDeequ analyzer computations."""

    def test_basic_analysis_returns_metrics(self, spark):
        df = create_sample_df(spark)
        result_df = run_basic_analysis(spark, df)
        assert result_df.count() >= 2
        metrics = {row["name"] for row in result_df.collect()}
        assert "Size" in metrics
        assert "Completeness" in metrics

    def test_completeness_of_b_is_one(self, spark):
        df = create_sample_df(spark)
        result_df = run_basic_analysis(spark, df)
        completeness_b = result_df.filter((F.col("name") == "Completeness") & (F.col("instance") == "b")).first()
        assert completeness_b is not None
        assert completeness_b["value"] == 1.0

    def test_completeness_of_c_is_partial(self, spark):
        """Column c has 1 null out of 3 rows → completeness ≈ 0.667."""
        df = create_sample_df(spark)
        result_df = run_basic_analysis(spark, df)
        completeness_c = result_df.filter((F.col("name") == "Completeness") & (F.col("instance") == "c")).first()
        assert completeness_c is not None
        assert round(completeness_c["value"], 2) == 0.67

    def test_size_equals_row_count(self, spark):
        df = create_sample_df(spark)
        result_df = run_basic_analysis(spark, df)
        size_row = result_df.filter(F.col("name") == "Size").first()
        assert size_row is not None
        assert size_row["value"] == 3.0

    def test_retail_analysis_returns_metrics(self, spark):
        df = create_retail_df(spark)
        result_df = run_retail_analysis(spark, df)
        assert result_df.count() >= 4
        metrics = {row["name"] for row in result_df.collect()}
        assert "Size" in metrics
        assert "Completeness" in metrics
        assert "Mean" in metrics

    def test_retail_size_equals_row_count(self, spark):
        df = create_retail_df(spark)
        result_df = run_retail_analysis(spark, df)
        size_row = result_df.filter(F.col("name") == "Size").first()
        assert size_row["value"] == 8.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
