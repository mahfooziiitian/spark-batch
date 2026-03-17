import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from column.has_column import has_column

SCHEMA = StructType([
    StructField("rollno",  StringType(), nullable=False),
    StructField("name",    StringType(), nullable=True),
    StructField("metrics", StructType([
        StructField("age",    IntegerType(), nullable=True),
        StructField("height", FloatType(),   nullable=True),
    ]), nullable=True),
    StructField("address", StringType(), nullable=True),
])

DATA = [("001", "Alice", (23, 5.79), "New York")]


class TestFieldNames:
    def test_field_names_top_level(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert "rollno"  in df.schema.fieldNames()
        assert "name"    in df.schema.fieldNames()
        assert "metrics" in df.schema.fieldNames()
        assert "address" in df.schema.fieldNames()

    def test_missing_column_not_in_field_names(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert "missing" not in df.schema.fieldNames()

    def test_struct_field_contains_exact_match(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        target = StructField("name", StringType(), True)
        assert target in df.schema.fields

    def test_struct_field_wrong_nullable_no_match(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        wrong_nullable = StructField("name", StringType(), False)
        assert wrong_nullable not in df.schema.fields

    def test_nested_field_names_accessible(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        nested = df.schema["metrics"].dataType.fieldNames()
        assert "age"    in nested
        assert "height" in nested
        assert "bmi"    not in nested


class TestHasColumn:
    def test_top_level_exists(self, spark):
        df = spark.sparkContext.parallelize(
            [Row(foo=[Row(bar=Row(foobar=3))])]
        ).toDF()
        assert has_column(df, "foo") is True

    def test_top_level_missing(self, spark):
        df = spark.sparkContext.parallelize(
            [Row(foo=[Row(bar=Row(foobar=3))])]
        ).toDF()
        assert has_column(df, "foobar") is False

    def test_nested_path_exists(self, spark):
        df = spark.sparkContext.parallelize(
            [Row(foo=[Row(bar=Row(foobar=3))])]
        ).toDF()
        assert has_column(df, "foo.bar")        is True
        assert has_column(df, "foo.bar.foobar") is True

    def test_nested_path_missing(self, spark):
        df = spark.sparkContext.parallelize(
            [Row(foo=[Row(bar=Row(foobar=3))])]
        ).toDF()
        assert has_column(df, "foo.bar.foobaz") is False

    def test_flat_schema_all_present(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        for col in ["rollno", "name", "address"]:
            assert has_column(df, col) is True

    def test_flat_schema_absent(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert has_column(df, "email") is False

    def test_returns_bool(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        result = has_column(df, "name")
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
