import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from schema_flattening import flatten_df, flatten_schema

SCHEMA = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("customer", StructType([
        StructField("id",   LongType(),   nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("address", StructType([
            StructField("city",    StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
        ]), nullable=True),
    ]), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("tags",   ArrayType(StringType()), nullable=True),
])

DATA = [
    (1, (10, "Alice", ("New York", "US")), 150.0, ["vip"]),
    (2, (11, "Bob",   ("London",   "UK")), 200.0, ["new"]),
]


class TestFlattenSchema:
    def test_returns_list_of_tuples(self):
        paths = flatten_schema(SCHEMA)
        assert isinstance(paths, list)
        assert all(isinstance(p, tuple) and len(p) == 2 for p in paths)

    def test_leaf_paths_no_struct_type(self):
        paths = flatten_schema(SCHEMA)
        # StructType fields should be recursed into, not listed as leaf nodes
        for path, _ in paths:
            assert "customer" != path        # not the struct itself
            assert "customer.address" != path

    def test_nested_paths_present(self):
        paths = [p for p, _ in flatten_schema(SCHEMA)]
        assert "customer.id"              in paths
        assert "customer.name"            in paths
        assert "customer.address.city"    in paths
        assert "customer.address.country" in paths

    def test_scalar_top_level_present(self):
        paths = [p for p, _ in flatten_schema(SCHEMA)]
        assert "order_id" in paths
        assert "amount"   in paths

    def test_array_listed_as_leaf(self):
        # ArrayType columns are NOT recursed into
        paths = [p for p, _ in flatten_schema(SCHEMA)]
        assert "tags" in paths

    def test_type_strings_non_empty(self):
        for _, type_str in flatten_schema(SCHEMA):
            assert len(type_str) > 0

    def test_flat_schema_no_nesting(self):
        flat = StructType([
            StructField("a", StringType(), nullable=True),
            StructField("b", LongType(),   nullable=True),
        ])
        paths = [p for p, _ in flatten_schema(flat)]
        assert paths == ["a", "b"]


class TestFlattenDf:
    def test_flat_df_no_struct_columns(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        flat = flatten_df(df)
        for col in flat.columns:
            assert not isinstance(flat.schema[col].dataType, StructType)

    def test_flat_df_contains_nested_columns(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        flat = flatten_df(df)
        assert "customer_id"              in flat.columns
        assert "customer_name"            in flat.columns
        assert "customer_address_city"    in flat.columns
        assert "customer_address_country" in flat.columns

    def test_flat_df_row_count_preserved(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        assert flatten_df(df).count() == df.count()

    def test_flat_df_values_correct(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        flat = flatten_df(df)
        row = flat.filter(F.col("order_id") == 1).first()
        assert row["customer_name"]            == "Alice"
        assert row["customer_address_city"]    == "New York"
        assert row["customer_address_country"] == "US"

    def test_flat_df_top_level_values(self, spark):
        df = spark.createDataFrame(DATA, schema=SCHEMA)
        flat = flatten_df(df)
        row = flat.filter(F.col("order_id") == 2).first()
        assert row["amount"] == 200.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
