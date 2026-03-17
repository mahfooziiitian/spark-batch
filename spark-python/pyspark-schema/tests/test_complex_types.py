import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


STUDENT_SCHEMA = StructType([
    StructField("rollno",  StringType(), nullable=False),
    StructField("name",    StringType(), nullable=True),
    StructField("metrics", StructType([
        StructField("age",    IntegerType(), nullable=True),
        StructField("height", FloatType(),   nullable=True),
        StructField("weight", IntegerType(), nullable=True),
    ]), nullable=True),
    StructField("address", StringType(), nullable=True),
])

STUDENT_DATA = [
    ("001", "Alice", (23, 5.79, 67), "New York"),
    ("002", "Bob",   (16, 3.79, 34), "London"),
    ("003", "Carol", (7,  2.79, 17), "Berlin"),
]

SCORE_SCHEMA = StructType([
    StructField("subject", StringType(), nullable=False),
    StructField("score",   DoubleType(), nullable=True),
])

NESTED_ARRAY_SCHEMA = StructType([
    StructField("id",     LongType(),              nullable=False),
    StructField("name",   StringType(),            nullable=True),
    StructField("scores", ArrayType(SCORE_SCHEMA), nullable=True),
])

NESTED_DATA = [
    (1, "Alice", [("maths", 95.0), ("science", 88.0)]),
    (2, "Bob",   [("maths", 72.0), ("science", 91.0)]),
]


class TestArraySchema:
    def test_array_of_primitives_type(self, spark):
        schema = StructType([
            StructField("id",   LongType(),              nullable=False),
            StructField("tags", ArrayType(StringType()), nullable=True),
        ])
        df = spark.createDataFrame([], schema)
        assert isinstance(df.schema["tags"].dataType, ArrayType)
        assert isinstance(df.schema["tags"].dataType.elementType, StringType)

    def test_array_row_count(self, spark):
        schema = StructType([
            StructField("id",   LongType(),              nullable=False),
            StructField("tags", ArrayType(StringType()), nullable=True),
        ])
        data = [(1, ["a", "b"]), (2, ["c"])]
        df = spark.createDataFrame(data, schema)
        assert df.count() == 2

    def test_array_size_function(self, spark):
        schema = StructType([
            StructField("id",   LongType(),              nullable=False),
            StructField("tags", ArrayType(StringType()), nullable=True),
        ])
        data = [(1, ["a", "b", "c"]), (2, ["x"])]
        df = spark.createDataFrame(data, schema)
        result = df.withColumn("n", F.size("tags"))
        row = result.filter(F.col("id") == 1).first()
        assert row["n"] == 3

    def test_array_contains(self, spark):
        schema = StructType([
            StructField("id",   LongType(),              nullable=False),
            StructField("tags", ArrayType(StringType()), nullable=True),
        ])
        data = [(1, ["spark", "python"]), (2, ["java"])]
        df = spark.createDataFrame(data, schema)
        result = df.withColumn("has_spark", F.array_contains("tags", "spark"))
        assert result.filter(F.col("has_spark")).count() == 1

    def test_explode_row_count(self, spark):
        schema = StructType([
            StructField("id",   LongType(),              nullable=False),
            StructField("tags", ArrayType(StringType()), nullable=True),
        ])
        data = [(1, ["a", "b"]), (2, ["c", "d", "e"])]
        df = spark.createDataFrame(data, schema)
        exploded = df.withColumn("tag", F.explode("tags")).select("id", "tag")
        assert exploded.count() == 5

    def test_array_of_structs_schema(self, spark):
        df = spark.createDataFrame(NESTED_DATA, schema=NESTED_ARRAY_SCHEMA)
        scores_type = df.schema["scores"].dataType
        assert isinstance(scores_type, ArrayType)
        assert isinstance(scores_type.elementType, StructType)
        assert scores_type.elementType.fieldNames() == ["subject", "score"]

    def test_array_of_structs_data(self, spark):
        df = spark.createDataFrame(NESTED_DATA, schema=NESTED_ARRAY_SCHEMA)
        assert df.count() == 2


class TestNestedStructs:
    def test_top_level_columns(self, spark):
        df = spark.createDataFrame(STUDENT_DATA, schema=STUDENT_SCHEMA)
        assert set(df.columns) == {"rollno", "name", "metrics", "address"}

    def test_nested_field_names(self, spark):
        df = spark.createDataFrame(STUDENT_DATA, schema=STUDENT_SCHEMA)
        nested = df.schema["metrics"].dataType
        assert isinstance(nested, StructType)
        assert nested.fieldNames() == ["age", "height", "weight"]

    def test_dot_notation_access(self, spark):
        df = spark.createDataFrame(STUDENT_DATA, schema=STUDENT_SCHEMA)
        result = df.select(F.col("metrics.age").alias("age"))
        row = result.filter(F.col("age") == 23).first()
        assert row is not None

    def test_dot_notation_filter(self, spark):
        df = spark.createDataFrame(STUDENT_DATA, schema=STUDENT_SCHEMA)
        assert df.filter(F.col("metrics.age") > 10).count() == 2

    def test_nested_type_is_struct(self, spark):
        df = spark.createDataFrame(STUDENT_DATA, schema=STUDENT_SCHEMA)
        assert isinstance(df.schema["metrics"].dataType, StructType)

    def test_struct_field_nullable(self, spark):
        df = spark.createDataFrame(STUDENT_DATA, schema=STUDENT_SCHEMA)
        assert df.schema["rollno"].nullable is False
        assert df.schema["name"].nullable   is True

    def test_flatten_nested_select(self, spark):
        df = spark.createDataFrame(STUDENT_DATA, schema=STUDENT_SCHEMA)
        flat = df.select(
            "rollno", "name",
            F.col("metrics.age").alias("age"),
            F.col("metrics.height").alias("height"),
        )
        assert flat.count() == 3
        assert set(flat.columns) == {"rollno", "name", "age", "height"}


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
