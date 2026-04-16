"""Tests for PyDeequ column profiling."""

import pytest

from dpu.mertics.computations.profiles.mertics_profile import profile_columns
from dpu.sample_data import create_retail_df, create_sample_df


class TestProfiles:
    """Tests for ColumnProfilerRunner."""

    def test_basic_profiles_returns_all_columns(self, spark):
        df = create_sample_df(spark)
        result = profile_columns(spark, df)
        assert set(result.profiles.keys()) == {"a", "b", "c"}

    def test_profile_has_completeness(self, spark):
        """Every profile should report a completeness ratio."""
        df = create_sample_df(spark)
        result = profile_columns(spark, df)
        for _col, profile in result.profiles.items():
            assert hasattr(profile, "completeness")

    def test_column_c_completeness_is_partial(self, spark):
        """Column c has 1 null out of 3 → completeness ≈ 0.667."""
        df = create_sample_df(spark)
        result = profile_columns(spark, df)
        assert round(result.profiles["c"].completeness, 2) == 0.67

    def test_column_b_completeness_is_full(self, spark):
        df = create_sample_df(spark)
        result = profile_columns(spark, df)
        assert result.profiles["b"].completeness == 1.0

    def test_retail_profiles_returns_all_columns(self, spark):
        df = create_retail_df(spark)
        result = profile_columns(spark, df)
        assert set(result.profiles.keys()) == {"product", "category", "price", "quantity", "region"}


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
