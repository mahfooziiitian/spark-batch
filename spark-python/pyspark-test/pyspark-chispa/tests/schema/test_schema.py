import pytest
from chispa import assert_df_equality
from chispa.schema_comparer import assert_schema_equality
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from data_frame.schema.schema_utils import add_nullable_fields, get_column_names_by_type, schema_to_dict


class TestSchemaEquality:
    """Tests demonstrating chispa schema comparison."""

    def test_matching_schemas(self, spark):
        df1 = spark.createDataFrame([(1, 4), (2, 3)], ["num", "val"])
        df2 = spark.createDataFrame([(5, 6), (7, 8)], ["num", "val"])
        assert_schema_equality(df1.schema, df2.schema)

    def test_mismatched_schemas_raises(self, spark):
        df1 = spark.createDataFrame([(1, "a")], ["num", "letter"])
        df2 = spark.createDataFrame([(1, 6)], ["num", "num2"])
        with pytest.raises(Exception):
            assert_df_equality(df1, df2)

    def test_same_columns_different_types_raises(self, spark):
        schema1 = StructType([StructField("id", LongType(), True), StructField("val", StringType(), True)])
        schema2 = StructType([StructField("id", LongType(), True), StructField("val", DoubleType(), True)])
        with pytest.raises(Exception):
            assert_schema_equality(schema1, schema2)

    def test_different_column_count_raises(self, spark):
        schema1 = StructType([StructField("a", LongType(), True)])
        schema2 = StructType([StructField("a", LongType(), True), StructField("b", LongType(), True)])
        with pytest.raises(Exception):
            assert_schema_equality(schema1, schema2)

    def test_empty_schemas_match(self, spark):
        schema1 = StructType([])
        schema2 = StructType([])
        assert_schema_equality(schema1, schema2)

    def test_nested_struct_schemas(self, spark):
        nested = StructType([
            StructField("id", LongType(), True),
            StructField("info", StructType([StructField("name", StringType(), True)]), True),
        ])
        assert_schema_equality(nested, nested)


class TestGetColumnNamesByType:
    """Tests for get_column_names_by_type schema utility."""

    def test_filters_string_columns(self, spark):
        df = spark.createDataFrame([(1, "a", 2.0)], ["id", "name", "score"])
        result = get_column_names_by_type(df, "string")
        assert result == ["name"]

    def test_filters_long_columns(self, spark):
        df = spark.createDataFrame([(1, "a", 2)], ["id", "name", "count"])
        result = get_column_names_by_type(df, "long")
        assert result == ["id", "count"]

    def test_no_matching_type(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        result = get_column_names_by_type(df, "string")
        assert result == []

    def test_all_matching_type(self, spark):
        df = spark.createDataFrame([("a", "b", "c")], ["x", "y", "z"])
        result = get_column_names_by_type(df, "string")
        assert result == ["x", "y", "z"]


class TestSchemaToDict:
    """Tests for schema_to_dict conversion utility."""

    def test_basic_schema(self, spark):
        schema = StructType([
            StructField("id", LongType()),
            StructField("name", StringType()),
        ])
        assert schema_to_dict(schema) == {"id": "long", "name": "string"}

    def test_empty_schema(self, spark):
        assert schema_to_dict(StructType([])) == {}

    def test_mixed_types(self, spark):
        schema = StructType([
            StructField("a", LongType()),
            StructField("b", DoubleType()),
            StructField("c", StringType()),
        ])
        result = schema_to_dict(schema)
        assert result == {"a": "long", "b": "double", "c": "string"}


class TestAddNullableFields:
    """Tests for add_nullable_fields schema utility."""

    def test_makes_all_fields_nullable(self, spark):
        schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField("name", StringType(), nullable=False),
        ])
        result = add_nullable_fields(schema)
        for field in result.fields:
            assert field.nullable is True

    def test_already_nullable_unchanged(self, spark):
        schema = StructType([
            StructField("id", LongType(), nullable=True),
        ])
        result = add_nullable_fields(schema)
        assert_schema_equality(result, schema)

    def test_preserves_field_names_and_types(self, spark):
        schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField("score", DoubleType(), nullable=False),
        ])
        result = add_nullable_fields(schema)
        assert schema_to_dict(result) == {"id": "long", "score": "double"}

    def test_empty_schema(self, spark):
        result = add_nullable_fields(StructType([]))
        assert result == StructType([])


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

