"""Tests for spark_reader using mock-based approach."""

from unittest.mock import Mock

import pytest

from pys.reader.spark_reader import load_csv, load_csv_with_schema, load_json, load_parquet


@pytest.fixture
def mock_spark():
    """Create a Mock SparkSession with chained read API."""
    spark_mock = Mock()
    spark_mock.read.format.return_value = spark_mock.read
    spark_mock.read.option.return_value = spark_mock.read
    spark_mock.read.schema.return_value = spark_mock.read
    spark_mock.read.load.return_value = Mock(name="result_df")
    spark_mock.read.parquet.return_value = Mock(name="parquet_df")
    return spark_mock


class TestLoadCsv:
    """Tests for CSV reader using mocks."""

    def test_uses_csv_format(self, mock_spark):
        """load_csv uses csv format."""
        load_csv(mock_spark, "/tmp/test.csv")
        mock_spark.read.format.assert_called_with("csv")

    def test_sets_separator(self, mock_spark):
        """load_csv sets comma separator option."""
        load_csv(mock_spark, "/tmp/test.csv")
        mock_spark.read.option.assert_any_call("sep", ",")

    def test_sets_header(self, mock_spark):
        """load_csv enables header option."""
        load_csv(mock_spark, "/tmp/test.csv")
        mock_spark.read.option.assert_any_call("header", "true")

    def test_loads_file_path(self, mock_spark):
        """load_csv passes the correct file path to load."""
        load_csv(mock_spark, "/data/peoples.csv")
        mock_spark.read.load.assert_called_with("/data/peoples.csv")

    def test_sets_infer_schema(self, mock_spark):
        """load_csv enables schema inference."""
        load_csv(mock_spark, "/tmp/test.csv")
        mock_spark.read.option.assert_any_call("inferSchema", "true")


class TestLoadCsvWithSchema:
    """Tests for CSV reader with explicit schema."""

    def test_uses_csv_format(self, mock_spark):
        """load_csv_with_schema uses csv format."""
        schema = Mock(name="schema")
        load_csv_with_schema(mock_spark, "/tmp/test.csv", schema)
        mock_spark.read.format.assert_called_with("csv")

    def test_applies_schema(self, mock_spark):
        """load_csv_with_schema applies the provided schema."""
        schema = Mock(name="schema")
        load_csv_with_schema(mock_spark, "/tmp/test.csv", schema)
        mock_spark.read.schema.assert_called_with(schema)

    def test_does_not_infer_schema(self, mock_spark):
        """load_csv_with_schema does not set inferSchema option."""
        schema = Mock(name="schema")
        load_csv_with_schema(mock_spark, "/tmp/test.csv", schema)
        infer_calls = [
            call for call in mock_spark.read.option.call_args_list if call[0][0] == "inferSchema"
        ]
        assert len(infer_calls) == 0


class TestLoadJson:
    """Tests for JSON reader using mocks."""

    def test_uses_json_format(self, mock_spark):
        """load_json uses json format."""
        load_json(mock_spark, "/tmp/data.json")
        mock_spark.read.format.assert_called_with("json")

    def test_sets_multiline(self, mock_spark):
        """load_json enables multiLine option."""
        load_json(mock_spark, "/tmp/data.json")
        mock_spark.read.option.assert_any_call("multiLine", "true")

    def test_loads_file_path(self, mock_spark):
        """load_json passes the correct file path."""
        load_json(mock_spark, "/data/records.json")
        mock_spark.read.load.assert_called_with("/data/records.json")


class TestLoadParquet:
    """Tests for Parquet reader using mocks."""

    def test_calls_parquet(self, mock_spark):
        """load_parquet calls spark.read.parquet with correct path."""
        load_parquet(mock_spark, "/data/output.parquet")
        mock_spark.read.parquet.assert_called_with("/data/output.parquet")

    def test_returns_dataframe(self, mock_spark):
        """load_parquet returns the DataFrame from parquet read."""
        result = load_parquet(mock_spark, "/data/output.parquet")
        assert result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
