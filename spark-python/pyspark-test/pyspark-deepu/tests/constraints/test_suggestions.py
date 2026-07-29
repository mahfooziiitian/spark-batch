"""Tests for PyDeequ ConstraintSuggestionRunner."""

import pytest
from pydeequ.suggestions import DEFAULT, ConstraintSuggestionRunner
from pyspark.sql import Row


class TestConstraintSuggestions:
    """Tests for automatic constraint suggestion."""

    def test_suggestions_returns_dict(self, spark):
        """Suggestion runner returns a dict result."""
        df = spark.createDataFrame([Row(a="foo", b=1), Row(a="bar", b=2), Row(a="baz", b=3)])
        result = ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT()).run()
        assert isinstance(result, dict)

    def test_suggestions_contain_constraints(self, spark):
        """Suggestions include constraint entries for data columns."""
        df = spark.createDataFrame(
            [Row(name="alice", age=30), Row(name="bob", age=25), Row(name="carol", age=35)]
        )
        result = ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT()).run()
        assert "constraint_suggestions" in result or len(result) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
