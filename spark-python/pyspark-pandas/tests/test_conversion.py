"""Tests for DataFrame conversion between pandas, Spark, and pandas-on-Spark."""

import pandas as pd
import pyspark.pandas as ps
import pytest


class TestConversion:
    def test_pandas_to_spark(self, spark):
        pdf = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        sdf = spark.createDataFrame(pdf)
        assert sdf.count() == 3
        assert set(sdf.columns) == {"id", "val"}

    def test_spark_to_pandas(self, spark):
        sdf = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        pdf = sdf.toPandas()
        assert isinstance(pdf, pd.DataFrame)
        assert len(pdf) == 2

    def test_pandas_to_ps(self, spark):
        pdf = pd.DataFrame({"x": [10, 20, 30]})
        psdf = ps.from_pandas(pdf)
        assert isinstance(psdf, ps.DataFrame)
        assert len(psdf) == 3

    def test_ps_to_pandas(self, spark):
        psdf = ps.DataFrame({"x": [10, 20, 30]})
        pdf = psdf.to_pandas()
        assert isinstance(pdf, pd.DataFrame)
        assert len(pdf) == 3

    def test_spark_to_ps(self, spark):
        sdf = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        psdf = sdf.pandas_api()
        assert isinstance(psdf, ps.DataFrame)
        assert len(psdf) == 3

    def test_ps_to_spark(self, spark):
        psdf = ps.DataFrame({"id": [1, 2, 3]})
        sdf = psdf.to_spark()
        assert sdf.count() == 3

    def test_round_trip_preserves_data(self, spark):
        pdf = pd.DataFrame({"id": [1, 2, 3], "score": [10.0, 20.0, 30.0]})
        sdf = spark.createDataFrame(pdf)
        result = sdf.toPandas()
        pd.testing.assert_frame_equal(
            pdf.sort_values("id").reset_index(drop=True),
            result.sort_values("id").reset_index(drop=True),
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
