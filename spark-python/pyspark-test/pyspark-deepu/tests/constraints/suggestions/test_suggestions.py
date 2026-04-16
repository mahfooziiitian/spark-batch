"""Tests for PyDeequ constraint suggestions."""

import pytest

from dpu.constraints.suggestions.constraint_suggestions import suggest_constraints
from dpu.sample_data import create_retail_df, create_sample_df


class TestSuggestions:
    """Tests for ConstraintSuggestionRunner."""

    def test_basic_suggestions_returns_dict(self, spark):
        df = create_sample_df(spark)
        result = suggest_constraints(spark, df)
        assert isinstance(result, dict)

    def test_basic_suggestions_has_suggestions_key(self, spark):
        df = create_sample_df(spark)
        result = suggest_constraints(spark, df)
        assert "constraint_suggestions" in result

    def test_basic_suggestions_not_empty(self, spark):
        df = create_sample_df(spark)
        result = suggest_constraints(spark, df)
        assert len(result["constraint_suggestions"]) > 0

    def test_retail_suggestions_returns_results(self, spark):
        df = create_retail_df(spark)
        result = suggest_constraints(spark, df)
        assert "constraint_suggestions" in result
        assert len(result["constraint_suggestions"]) > 0

    def test_suggestion_has_description(self, spark):
        """Each suggestion should contain a description field."""
        df = create_sample_df(spark)
        result = suggest_constraints(spark, df)
        for suggestion in result["constraint_suggestions"]:
            assert "description" in suggestion


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
