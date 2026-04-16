"""Tests for pandas-on-Spark DataFrame and Series creation."""

import numpy as np
import pyspark.pandas as ps
import pytest


class TestPandasCreation:
    def test_dataframe_from_dict(self, spark):
        psdf = ps.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6],
                "b": [100, 200, 300, 400, 500, 600],
                "c": ["one", "two", "three", "four", "five", "six"],
            },
            index=[10, 20, 30, 40, 50, 60],
        )
        assert isinstance(psdf, ps.DataFrame)
        assert len(psdf) == 6
        assert set(psdf.columns) == {"a", "b", "c"}

    def test_series_creation(self, spark):
        pss = ps.Series([1, 3, 5, np.nan, 6, 8])
        assert isinstance(pss, ps.Series)
        assert len(pss) == 6

    def test_dataframe_from_range(self, spark):
        psdf = ps.range(10)
        assert isinstance(psdf, ps.DataFrame)
        assert len(psdf) == 10
        assert "id" in psdf.columns

    def test_dataframe_from_pandas(self, spark):
        import pandas as pd

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
        psdf = ps.from_pandas(pdf)
        assert isinstance(psdf, ps.DataFrame)
        assert len(psdf) == 3
        assert set(psdf.columns) == {"x", "y"}


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
