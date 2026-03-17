import json
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class TestStructFieldList:
    def test_field_names(self, spark):
        schema = StructType([
            StructField("id",   LongType(),   nullable=False),
            StructField("name", StringType(), nullable=True),
        ])
        df = spark.createDataFrame([], schema)
        assert df.schema.fieldNames() == ["id", "name"]

    def test_field_types(self, spark):
        schema = StructType([
            StructField("id",     LongType(),   nullable=False),
            StructField("amount", DoubleType(), nullable=True),
        ])
        df = spark.createDataFrame([], schema)
        type_map = {f.name: type(f.dataType) for f in df.schema.fields}
        assert type_map["id"]     is LongType
        assert type_map["amount"] is DoubleType

    def test_nullable_flags(self, spark):
        schema = StructType([
            StructField("id",   LongType(),   nullable=False),
            StructField("name", StringType(), nullable=True),
        ])
        df = spark.createDataFrame([], schema)
        nullable_map = {f.name: f.nullable for f in df.schema.fields}
        assert nullable_map["id"]   is False
        assert nullable_map["name"] is True

    def test_field_count(self, spark):
        schema = StructType([
            StructField("a", StringType(), nullable=True),
            StructField("b", StringType(), nullable=True),
            StructField("c", StringType(), nullable=True),
        ])
        df = spark.createDataFrame([], schema)
        assert len(df.schema.fields) == 3

    def test_data_roundtrip(self, spark):
        schema = StructType([
            StructField("id",   LongType(),   nullable=False),
            StructField("name", StringType(), nullable=True),
        ])
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema)
        assert df.count() == 2
        assert df.filter(df["name"] == "Alice").count() == 1


class TestBuilderPattern:
    def test_untyped_builder(self, spark):
        schema = (StructType()
                  .add("id",   "long",   nullable=False)
                  .add("name", "string", nullable=True))
        df = spark.createDataFrame([], schema)
        assert set(df.columns) == {"id", "name"}

    def test_typed_builder(self, spark):
        schema = (StructType()
                  .add("id",   LongType(),   nullable=False)
                  .add("name", StringType(), nullable=True))
        df = spark.createDataFrame([], schema)
        assert isinstance(df.schema["id"].dataType, LongType)

    def test_typed_and_untyped_equal(self):
        untyped = (StructType()
                   .add("id",   "long",    nullable=False)
                   .add("name", "string",  nullable=True))
        typed = (StructType()
                 .add("id",   LongType(),   nullable=False)
                 .add("name", StringType(), nullable=True))
        assert untyped.simpleString() == typed.simpleString()

    def test_builder_chaining_returns_same_object(self):
        st = StructType()
        result = st.add("a", "string")
        assert result is st


class TestDDLAndMapType:
    def test_from_ddl_field_names(self, spark):
        from pyspark.sql.types import _parse_datatype_string
        schema = _parse_datatype_string("struct<id:bigint not null,name:string>")
        df = spark.createDataFrame([], schema)
        assert "id" in df.columns
        assert "name" in df.columns

    def test_from_ddl_not_null(self, spark):
        from pyspark.sql.types import _parse_datatype_string
        schema = _parse_datatype_string("struct<id:bigint not null,name:string>")
        assert schema["id"].nullable   is False
        assert schema["name"].nullable is True

    def test_map_type_structure(self, spark):
        schema = StructType([
            StructField("id",    LongType(),                          nullable=False),
            StructField("props", MapType(StringType(), StringType()), nullable=True),
        ])
        df = spark.createDataFrame([], schema)
        props_field = df.schema["props"]
        assert isinstance(props_field.dataType, MapType)
        assert isinstance(props_field.dataType.keyType,   StringType)
        assert isinstance(props_field.dataType.valueType, StringType)

    def test_map_type_data(self, spark):
        schema = StructType([
            StructField("id",    LongType(),                          nullable=False),
            StructField("props", MapType(StringType(), StringType()), nullable=True),
        ])
        data = [(1, {"color": "red"}), (2, {"size": "L"})]
        df = spark.createDataFrame(data, schema)
        assert df.count() == 2


class TestFromJson:
    REGISTRY = {
        "type": "struct",
        "fields": [
            {"name": "id",   "type": "long",   "nullable": False, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True,  "metadata": {}},
        ],
    }

    def test_from_json_field_names(self):
        schema = StructType.fromJson(self.REGISTRY)
        assert schema.fieldNames() == ["id", "name"]

    def test_from_json_nullable(self):
        schema = StructType.fromJson(self.REGISTRY)
        assert schema["id"].nullable   is False
        assert schema["name"].nullable is True

    def test_json_roundtrip(self):
        schema = StructType.fromJson(self.REGISTRY)
        schema_back = StructType.fromJson(json.loads(schema.json()))
        assert schema == schema_back

    def test_from_json_creates_dataframe(self, spark):
        schema = StructType.fromJson(self.REGISTRY)
        df = spark.createDataFrame([(1, "Alice")], schema)
        assert df.count() == 1


class TestDecimalType:
    def test_precision_scale(self, spark):
        schema = StructType([
            StructField("amount", DecimalType(18, 2), nullable=True),
        ])
        df = spark.createDataFrame([], schema)
        dt = df.schema["amount"].dataType
        assert dt.precision == 18
        assert dt.scale == 2

    def test_decimal_simple_string(self):
        assert DecimalType(10, 2).simpleString() == "decimal(10,2)"

    def test_decimal_sum_exact(self, spark):
        from pyspark.sql import functions as F

        schema = StructType([StructField("v", DecimalType(10, 2), nullable=True)])
        data = [(Decimal("0.10"),), (Decimal("0.20"),), (Decimal("0.30"),)]
        df = spark.createDataFrame(data, schema)
        total = df.agg(F.sum("v")).collect()[0][0]
        assert total == Decimal("0.60")

    def test_decimal_field_in_struct(self, spark):
        schema = StructType([
            StructField("price", DecimalType(10, 2), nullable=True),
            StructField("tax",   DecimalType(10, 4), nullable=True),
        ])
        df = spark.createDataFrame([], schema)
        assert isinstance(df.schema["price"].dataType, DecimalType)
        assert isinstance(df.schema["tax"].dataType,   DecimalType)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
