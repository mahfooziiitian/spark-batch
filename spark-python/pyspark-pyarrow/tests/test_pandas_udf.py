import pandas as pd
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


class TestScalarPandasUDF:
    """Series → Series UDF tests."""

    def test_element_wise_transform(self, spark):
        df = spark.createDataFrame([(100.0,), (200.0,), (300.0,)], ["salary"])

        @F.pandas_udf(DoubleType())
        def tax(s: pd.Series) -> pd.Series:
            return s * 0.25

        result = df.withColumn("tax", tax(F.col("salary")))
        assert result.count() == 3
        row = result.filter(F.col("salary") == 200.0).first()
        assert row["tax"] == 50.0

    def test_string_udf(self, spark):
        df = spark.createDataFrame([("hello",), ("world",)], ["word"])

        @F.pandas_udf(StringType())
        def shout(s: pd.Series) -> pd.Series:
            return s.str.upper() + "!"

        result = df.withColumn("shouted", shout(F.col("word")))
        row = result.filter(F.col("word") == "hello").first()
        assert row["shouted"] == "HELLO!"


class TestGroupedAggregatePandasUDF:
    """Series… → scalar UDF tests."""

    def test_grouped_mean(self, spark):
        data = [("A", 10.0), ("A", 20.0), ("B", 100.0)]
        schema = StructType([
            StructField("grp", StringType()),
            StructField("val", DoubleType()),
        ])
        df = spark.createDataFrame(data, schema=schema)

        @F.pandas_udf(DoubleType())
        def avg_val(v: pd.Series) -> float:
            return v.mean()

        result = df.groupBy("grp").agg(avg_val(F.col("val")).alias("avg"))
        assert result.count() == 2
        row_a = result.filter(F.col("grp") == "A").first()
        assert row_a["avg"] == 15.0


class TestGroupedMapApplyInPandas:
    """applyInPandas tests."""

    def test_normalise_within_group(self, spark):
        data = [("X", 10.0), ("X", 20.0), ("Y", 5.0), ("Y", 15.0)]
        schema = StructType([
            StructField("grp", StringType()),
            StructField("val", DoubleType()),
        ])
        df = spark.createDataFrame(data, schema=schema)

        result_schema = StructType([
            StructField("grp", StringType()),
            StructField("val", DoubleType()),
            StructField("centred", DoubleType()),
        ])

        def centre(pdf: pd.DataFrame) -> pd.DataFrame:
            pdf["centred"] = pdf["val"] - pdf["val"].mean()
            return pdf

        result = df.groupBy("grp").applyInPandas(centre, schema=result_schema)
        assert result.count() == 4

        row = result.filter((F.col("grp") == "X") & (F.col("val") == 10.0)).first()
        assert row["centred"] == -5.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
