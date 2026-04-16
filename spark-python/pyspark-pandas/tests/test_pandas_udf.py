"""Tests for pandas UDF patterns."""

import pandas as pd
import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType

from spp.pandas_udf.grouped_map_udf import (
    NORMALIZE_SCHEMA,
    normalize_within_group,
)


class TestPandasUDF:
    def test_series_to_series_udf(self, spark):
        @pandas_udf(StringType())
        def upper(s: pd.Series) -> pd.Series:
            return s.str.upper()

        df = spark.createDataFrame([("alice",), ("bob",)], ["name"])
        result = df.withColumn("upper", upper("name"))
        rows = {r["upper"] for r in result.collect()}
        assert rows == {"ALICE", "BOB"}

    def test_grouped_aggregate_udf(self, spark):
        @pandas_udf(DoubleType())
        def avg_udf(v: pd.Series) -> float:
            return v.mean()

        df = spark.createDataFrame([(1, 10.0), (1, 20.0), (2, 30.0)], ["grp", "val"])
        result = df.groupBy("grp").agg(avg_udf("val").alias("avg"))
        assert result.count() == 2
        row = result.filter(F.col("grp") == 1).first()
        assert row["avg"] == pytest.approx(15.0)

    def test_grouped_map_normalize(self, spark):
        df = spark.createDataFrame(
            [(1, "a", 10.0), (1, "b", 20.0), (1, "c", 30.0)],
            ["group_id", "name", "score"],
        )
        result = df.groupBy("group_id").applyInPandas(
            normalize_within_group, schema=NORMALIZE_SCHEMA
        )
        assert result.count() == 3
        assert "norm_score" in result.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
