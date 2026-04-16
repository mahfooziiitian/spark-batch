import pytest
from pyspark.sql import functions as F

from common import filter_senior_citizen, remove_extra_spaces


class TestRemoveExtraSpaces:
    def test_collapses_multiple_spaces(self, spark):
        data = [{"name": "John    D.", "age": 30},
                {"name": "Alice   G.", "age": 25},
                {"name": "Bob  T.", "age": 35},
                {"name": "Eve   A.", "age": 28}]
        df = spark.createDataFrame(data)
        result = remove_extra_spaces(df, "name")

        expected = [{"name": "John D.", "age": 30},
                    {"name": "Alice G.", "age": 25},
                    {"name": "Bob T.", "age": 35},
                    {"name": "Eve A.", "age": 28}]
        expected_df = spark.createDataFrame(expected)

        assert result.collect() == expected_df.collect()

    def test_preserves_row_count(self, spark):
        data = [{"name": "John    D.", "age": 30},
                {"name": "Alice   G.", "age": 25},
                {"name": "Bob  T.", "age": 35},
                {"name": "Eve   A.", "age": 28}]
        df = spark.createDataFrame(data)
        result = remove_extra_spaces(df, "name")
        assert result.count() == 4

    def test_no_change_when_single_spaces(self, spark):
        data = [{"name": "John D.", "age": 30}]
        df = spark.createDataFrame(data)
        result = remove_extra_spaces(df, "name")
        assert result.first()["name"] == "John D."


class TestFilterSeniorCitizen:
    def test_filters_age_60_and_above(self, spark):
        data = [{"name": "John D.", "age": 60},
                {"name": "Alice G.", "age": 25},
                {"name": "Bob T.", "age": 65},
                {"name": "Eve A.", "age": 28}]
        df = spark.createDataFrame(data)
        result = filter_senior_citizen(df, "age")
        assert result.count() == 2
        assert set(row["name"] for row in result.collect()) == {"John D.", "Bob T."}

    def test_includes_exact_boundary(self, spark):
        data = [{"name": "Boundary", "age": 60},
                {"name": "Below", "age": 59}]
        df = spark.createDataFrame(data)
        result = filter_senior_citizen(df, "age")
        assert result.count() == 1
        assert result.first()["name"] == "Boundary"

    def test_all_seniors(self, spark):
        data = [{"name": "John D.", "age": 60},
                {"name": "Bob T.", "age": 65},
                {"name": "Eve A.", "age": 66}]
        df = spark.createDataFrame(data)
        result = filter_senior_citizen(df, "age")
        assert result.count() == 3

    def test_no_seniors(self, spark):
        data = [{"name": "Alice G.", "age": 25},
                {"name": "Eve A.", "age": 28}]
        df = spark.createDataFrame(data)
        result = filter_senior_citizen(df, "age")
        assert result.count() == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
