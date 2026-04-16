import pytest


class TestDataFrame:
    """Tests for DataFrame creation and SQL queries."""

    def test_spark(self, spark):
        df = spark.createDataFrame(
            data=[
                ("a", 1),
                ("b", 2),
                ("c", 3),
            ],
            schema=["letter", "number"],
        )

        df.createOrReplaceTempView("diamonds")
        data = spark.sql("SELECT * FROM diamonds")
        assert data.collect()[0][1] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
