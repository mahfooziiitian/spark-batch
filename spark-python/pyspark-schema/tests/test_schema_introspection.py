import json

import pytest
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

SCHEMA = StructType([
    StructField("id",      LongType(),   nullable=False),
    StructField("name",    StringType(), nullable=True),
    StructField("amount",  DoubleType(), nullable=True),
])

DATA = [(1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Carol", 300.0)]

NESTED_SCHEMA = StructType([
    StructField("rollno",  StringType(), nullable=False),
    StructField("name",    StringType(), nullable=True),
    StructField("metrics", StructType([
        StructField("age",    IntegerType(), nullable=True),
        StructField("height", FloatType(),   nullable=True),
    ]), nullable=True),
])


class TestSchemaIntrospection:
    def test_columns_list(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert df.columns == ["id", "name", "amount"]

    def test_dtypes_keys(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        names = [t[0] for t in df.dtypes]
        assert names == ["id", "name", "amount"]

    def test_dtypes_values(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        dtype_map = dict(df.dtypes)
        assert dtype_map["id"]     == "bigint"
        assert dtype_map["name"]   == "string"
        assert dtype_map["amount"] == "double"

    def test_simple_string_contains_all_fields(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        ss = df.schema.simpleString()
        assert "id" in ss
        assert "name" in ss
        assert "amount" in ss

    def test_simple_string_format(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert df.schema.simpleString().startswith("struct<")

    def test_json_is_valid_json(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        parsed = json.loads(df.schema.json())
        assert parsed["type"] == "struct"
        assert isinstance(parsed["fields"], list)

    def test_json_field_count(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        parsed = json.loads(df.schema.json())
        assert len(parsed["fields"]) == 3

    def test_field_names_list(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert df.schema.fieldNames() == ["id", "name", "amount"]

    def test_schema_type_name(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert df.schema.typeName() == "struct"

    def test_nested_field_metadata_accessible(self, spark):
        df = spark.createDataFrame(
            [("001", "Alice", (23, 5.79))],
            schema=NESTED_SCHEMA,
        )
        metrics_dt = df.schema["metrics"].dataType
        assert isinstance(metrics_dt, StructType)
        assert "age" in metrics_dt.fieldNames()

    def test_json_roundtrip_equality(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        restored = StructType.fromJson(json.loads(df.schema.json()))
        assert df.schema == restored


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
