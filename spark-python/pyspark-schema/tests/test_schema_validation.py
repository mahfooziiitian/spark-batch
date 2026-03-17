import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from arrays.pyspark_array_schema_validate import cast_to_schema, validate_schema

EXPECTED = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])

VALID_DATA = [(1, "Alice", 100.0), (2, "Bob", 200.0)]

BAD_SCHEMA = StructType([
    StructField("id",     StringType(), nullable=True),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", StringType(), nullable=True),
])
BAD_DATA = [("x", "Alice", "not-a-number")]


class TestValidateSchema:
    def test_valid_schema_no_errors(self, spark):
        df = spark.createDataFrame(VALID_DATA, schema=EXPECTED)
        assert validate_schema(df, EXPECTED) == []

    def test_missing_column_detected(self, spark):
        partial = StructType([
            StructField("id",   LongType(),   nullable=False),
            StructField("name", StringType(), nullable=True),
        ])
        df = spark.createDataFrame([(1, "Alice")], schema=partial)
        errors = validate_schema(df, EXPECTED)
        assert any("amount" in e for e in errors)

    def test_type_mismatch_detected(self, spark):
        df = spark.createDataFrame(BAD_DATA, schema=BAD_SCHEMA)
        errors = validate_schema(df, EXPECTED)
        assert any("id" in e for e in errors)
        assert any("amount" in e for e in errors)

    def test_returns_list(self, spark):
        df = spark.createDataFrame(VALID_DATA, schema=EXPECTED)
        result = validate_schema(df, EXPECTED)
        assert isinstance(result, list)

    def test_extra_columns_not_flagged(self, spark):
        extra = StructType([
            StructField("id",     LongType(),   nullable=False),
            StructField("name",   StringType(), nullable=True),
            StructField("amount", DoubleType(), nullable=True),
            StructField("extra",  StringType(), nullable=True),
        ])
        df = spark.createDataFrame([(1, "Alice", 100.0, "x")], schema=extra)
        errors = validate_schema(df, EXPECTED)
        assert errors == []


class TestCastToSchema:
    def test_cast_preserves_row_count(self, spark):
        df = spark.createDataFrame(BAD_DATA, schema=BAD_SCHEMA)
        result = cast_to_schema(df, EXPECTED)
        assert result.count() == len(BAD_DATA)

    def test_cast_produces_correct_columns(self, spark):
        df = spark.createDataFrame(BAD_DATA, schema=BAD_SCHEMA)
        result = cast_to_schema(df, EXPECTED)
        assert set(result.columns) == {"id", "name", "amount"}

    def test_cast_invalid_value_becomes_null(self, spark):
        df = spark.createDataFrame(BAD_DATA, schema=BAD_SCHEMA)
        result = cast_to_schema(df, EXPECTED)
        null_amounts = result.filter(F.col("amount").isNull()).count()
        assert null_amounts == len(BAD_DATA)

    def test_cast_valid_data_unchanged(self, spark):
        df_valid = spark.createDataFrame(
            [(1, "Alice", 100.0)],
            schema=StructType([
                StructField("id",     StringType(), nullable=True),
                StructField("name",   StringType(), nullable=True),
                StructField("amount", StringType(), nullable=True),
            ])
        )
        result = cast_to_schema(df_valid, EXPECTED)
        row = result.first()
        assert row["id"]   == 1
        assert row["name"] == "Alice"


class TestNullCounts:
    def test_no_nulls_in_valid_data(self, spark):
        df = spark.createDataFrame(VALID_DATA, schema=EXPECTED)
        null_ids = df.filter(F.col("id").isNull()).count()
        assert null_ids == 0

    def test_null_count_per_column(self, spark):
        schema = StructType([
            StructField("id",   LongType(),   nullable=False),
            StructField("name", StringType(), nullable=True),
        ])
        df = spark.createDataFrame([(1, None), (2, "Bob")], schema)
        assert df.filter(F.col("name").isNull()).count() == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
