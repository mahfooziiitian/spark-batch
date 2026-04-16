import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pyspark import Row
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


class TestArrowConversion:
    """Pandas ↔ Spark round-trip with Arrow enabled."""

    def test_pandas_to_spark_schema(self, spark):
        pdf = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        sdf = spark.createDataFrame(pdf)
        assert set(sdf.columns) == {"id", "value"}

    def test_pandas_to_spark_data(self, spark):
        pdf = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        sdf = spark.createDataFrame(pdf)
        assert sdf.count() == 3
        row = sdf.filter(F.col("id") == 2).first()
        assert row["value"] == "b"

    def test_spark_to_pandas_roundtrip(self, spark):
        pdf = pd.DataFrame({"x": [10.0, 20.0, 30.0], "y": [1, 2, 3]})
        sdf = spark.createDataFrame(pdf)
        result = sdf.toPandas()
        assert len(result) == 3
        assert list(result["x"]) == [10.0, 20.0, 30.0]

    def test_pyarrow_table_intermediate(self, spark):
        """Pandas → PyArrow Table → Pandas → Spark."""
        pdf = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        table = pa.Table.from_pandas(pdf)
        sdf = spark.createDataFrame(table.to_pandas())
        assert sdf.count() == 3
        assert sdf.collect() == [
            Row(id=1, value="a"),
            Row(id=2, value="b"),
            Row(id=3, value="c"),
        ]

    def test_large_dataframe_roundtrip(self, spark):
        pdf = pd.DataFrame(np.random.rand(1000, 3), columns=["a", "b", "c"])
        sdf = spark.createDataFrame(pdf)
        assert sdf.count() == 1000
        assert set(sdf.columns) == {"a", "b", "c"}
        result = sdf.toPandas()
        assert len(result) == 1000


class TestMapInPandas:
    def test_batch_transform(self, spark):
        df = spark.createDataFrame([(1, 10.0), (2, 20.0), (3, 30.0)], ["id", "val"])

        def double_val(iterator):
            for batch in iterator:
                batch["val"] = batch["val"] * 2
                yield batch

        result = df.mapInPandas(double_val, schema=df.schema)
        assert result.count() == 3
        row = result.filter(F.col("id") == 2).first()
        assert row["val"] == 40.0


class TestApplyInPandas:
    def test_grouped_transform(self, spark):
        data = [("A", 10.0), ("A", 20.0), ("B", 30.0), ("B", 40.0)]
        schema = StructType([
            StructField("group", StringType()),
            StructField("val", DoubleType()),
        ])
        df = spark.createDataFrame(data, schema=schema)

        result_schema = StructType([
            StructField("group", StringType()),
            StructField("val", DoubleType()),
            StructField("group_mean", DoubleType()),
        ])

        def add_mean(pdf: pd.DataFrame) -> pd.DataFrame:
            pdf["group_mean"] = pdf["val"].mean()
            return pdf

        result = df.groupBy("group").applyInPandas(add_mean, schema=result_schema)
        assert result.count() == 4
        row_a = result.filter(F.col("group") == "A").first()
        assert row_a["group_mean"] == 15.0
        row_b = result.filter(F.col("group") == "B").first()
        assert row_b["group_mean"] == 35.0


class TestParquetArrowIO:
    def test_write_and_read_parquet(self, spark, tmp_path):
        path = str(tmp_path / "output.parquet")
        pdf = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        df = spark.createDataFrame(pdf)
        df.write.mode("overwrite").parquet(path)
        read_back = spark.read.parquet(path)
        assert read_back.count() == 3
        assert set(read_back.columns) == {"id", "name"}


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])