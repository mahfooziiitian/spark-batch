"""Tests for DataFrame creation and SQL queries."""

import pytest


class TestDataFrame:
    """Tests for basic DataFrame and SQL operations."""

    def test_create_dataframe_and_query(self, spark):
        """DataFrame can be queried via SQL after registering as temp view."""
        df = spark.createDataFrame(
            data=[("a", 1), ("b", 2), ("c", 3)],
            schema=["letter", "number"],
        )
        df.createOrReplaceTempView("test_letters")

        result = spark.sql("SELECT * FROM test_letters")
        assert result.count() == 3
        assert result.collect()[0][1] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
