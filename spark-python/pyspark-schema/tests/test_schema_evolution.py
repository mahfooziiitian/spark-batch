import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

V1 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])

V2 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True),
])

DATA_V1 = [(1, "Alice", 100.0),          (2, "Bob",   200.0)]
DATA_V2 = [(3, "Carol", 150.0, "North"), (4, "Dave",  250.0, "South")]


class TestMergeSchema:
    def test_merged_contains_all_columns(self, spark, tmp_path):
        path_v1 = str(tmp_path / "v1")
        path_v2 = str(tmp_path / "v2")
        spark.createDataFrame(DATA_V1, V1).write.mode("overwrite").parquet(path_v1)
        spark.createDataFrame(DATA_V2, V2).write.mode("overwrite").parquet(path_v2)

        merged = spark.read.option("mergeSchema", "true").parquet(path_v1, path_v2)
        assert set(merged.columns) == {"id", "name", "amount", "region"}

    def test_merged_row_count(self, spark, tmp_path):
        path_v1 = str(tmp_path / "v1")
        path_v2 = str(tmp_path / "v2")
        spark.createDataFrame(DATA_V1, V1).write.mode("overwrite").parquet(path_v1)
        spark.createDataFrame(DATA_V2, V2).write.mode("overwrite").parquet(path_v2)

        merged = spark.read.option("mergeSchema", "true").parquet(path_v1, path_v2)
        assert merged.count() == 4

    def test_v1_rows_have_null_region(self, spark, tmp_path):
        path_v1 = str(tmp_path / "v1")
        path_v2 = str(tmp_path / "v2")
        spark.createDataFrame(DATA_V1, V1).write.mode("overwrite").parquet(path_v1)
        spark.createDataFrame(DATA_V2, V2).write.mode("overwrite").parquet(path_v2)

        merged = spark.read.option("mergeSchema", "true").parquet(path_v1, path_v2)
        null_regions = merged.filter(F.col("region").isNull()).count()
        assert null_regions == len(DATA_V1)

    def test_v2_rows_have_non_null_region(self, spark, tmp_path):
        path_v1 = str(tmp_path / "v1")
        path_v2 = str(tmp_path / "v2")
        spark.createDataFrame(DATA_V1, V1).write.mode("overwrite").parquet(path_v1)
        spark.createDataFrame(DATA_V2, V2).write.mode("overwrite").parquet(path_v2)

        merged = spark.read.option("mergeSchema", "true").parquet(path_v1, path_v2)
        non_null_regions = merged.filter(F.col("region").isNotNull()).count()
        assert non_null_regions == len(DATA_V2)


class TestBackwardCompatParquet:
    def test_v1_reader_ignores_extra_column(self, spark, tmp_path):
        # v1 reader (V1 schema) reads v2 data — 'region' column dropped
        path = str(tmp_path / "v2")
        spark.createDataFrame(DATA_V2, V2).write.mode("overwrite").parquet(path)

        df = spark.read.schema(V1).parquet(path)
        assert set(df.columns) == {"id", "name", "amount"}
        assert df.count() == len(DATA_V2)

    def test_v2_reader_gets_nulls_for_missing_column(self, spark, tmp_path):
        # v2 reader (V2 schema) reads v1 data — 'region' becomes null
        path = str(tmp_path / "v1")
        spark.createDataFrame(DATA_V1, V1).write.mode("overwrite").parquet(path)

        df = spark.read.schema(V2).parquet(path)
        assert "region" in df.columns
        assert df.filter(F.col("region").isNull()).count() == len(DATA_V1)

    def test_parquet_preserves_schema(self, spark, tmp_path):
        path = str(tmp_path / "data")
        spark.createDataFrame(DATA_V1, V1).write.mode("overwrite").parquet(path)
        read_back = spark.read.parquet(path)
        assert set(read_back.columns) == set(V1.fieldNames())

    def test_parquet_write_read_row_count(self, spark, tmp_path):
        path = str(tmp_path / "data")
        spark.createDataFrame(DATA_V1, V1).write.mode("overwrite").parquet(path)
        assert spark.read.parquet(path).count() == len(DATA_V1)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
