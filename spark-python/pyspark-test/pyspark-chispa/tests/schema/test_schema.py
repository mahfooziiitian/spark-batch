import pytest
from chispa.schema_comparer import SchemasNotEqualError, assert_schema_equality
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from data_frame.schema.schema_utils import add_nullable_fields, get_column_names_by_type, schema_to_dict


class TestGetColumnNamesByType:
    """Tests for the get_column_names_by_type utility."""

    def test_string_columns(self, spark):
        df = spark.createDataFrame([(1, "a", 2.0)], ["id", "name", "score"])
        assert get_column_names_by_type(df, "string") == ["name"]

    def test_no_matching_type(self, spark):
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        assert get_column_names_by_type(df, "string") == []

    def test_multiple_matches(self, spark):
        df = spark.createDataFrame([("a", "b", 1)], ["x", "y", "z"])
        assert get_column_names_by_type(df, "string") == ["x", "y"]


class TestSchemaToDict:
    """Tests for the schema_to_dict utility."""

    def test_basic_schema(self, spark):
        df = spark.createDataFrame([(1, "a", 2.0)], ["id", "name", "score"])
        result = schema_to_dict(df.schema)
        assert result == {"id": "bigint", "name": "string", "score": "double"}

    def test_empty_schema(self):
        result = schema_to_dict(StructType([]))
        assert result == {}


class TestAddNullableFields:
    """Tests for the add_nullable_fields utility."""

    def test_makes_all_nullable(self):
        schema = StructType(
            [
                StructField("id", LongType(), nullable=False),
                StructField("name", StringType(), nullable=False),
            ]
        )
        result = add_nullable_fields(schema)
        for field in result.fields:
            assert field.nullable is True

    def test_preserves_types_and_names(self):
        schema = StructType(
            [
                StructField("id", LongType(), nullable=False),
                StructField("score", DoubleType(), nullable=False),
            ]
        )
        result = add_nullable_fields(schema)
        assert result[0].name == "id"
        assert result[0].dataType == LongType()
        assert result[1].name == "score"
        assert result[1].dataType == DoubleType()

    def test_schema_equality_after_nullable(self):
        strict = StructType(
            [
                StructField("id", LongType(), nullable=False),
                StructField("name", StringType(), nullable=False),
            ]
        )
        relaxed = StructType(
            [
                StructField("id", LongType(), nullable=True),
                StructField("name", StringType(), nullable=True),
            ]
        )
        assert_schema_equality(add_nullable_fields(strict), relaxed)


class TestSchemaEquality:
    """Tests for chispa schema equality assertions."""

    def test_matching_schemas(self, spark):
        df1 = spark.createDataFrame([(1, 4)], ["num", "val"])
        df2 = spark.createDataFrame([(5, 6)], ["num", "val"])
        assert_schema_equality(df1.schema, df2.schema)

    def test_mismatched_schemas_raises(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["num", "letter"])
        df2 = spark.createDataFrame([(1, 6)], ["num", "num2"])
        with pytest.raises(SchemasNotEqualError):
            assert_schema_equality(df1.schema, df2.schema)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
