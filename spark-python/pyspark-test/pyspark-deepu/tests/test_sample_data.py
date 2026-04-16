"""Tests for shared sample data helpers."""

import pytest

from dpu.sample_data import (
    RETAIL_ROWS,
    RETAIL_SCHEMA,
    SAMPLE_ROWS,
    SAMPLE_SCHEMA,
    create_retail_df,
    create_sample_df,
)


class TestSampleData:
    """Tests for the shared sample data module."""

    def test_sample_df_row_count(self, spark):
        df = create_sample_df(spark)
        assert df.count() == len(SAMPLE_ROWS)

    def test_sample_df_columns(self, spark):
        df = create_sample_df(spark)
        assert set(df.columns) == {"a", "b", "c"}

    def test_sample_df_schema_matches(self, spark):
        df = create_sample_df(spark)
        assert df.schema == SAMPLE_SCHEMA

    def test_sample_df_has_null_in_c(self, spark):
        from pyspark.sql import functions as F

        df = create_sample_df(spark)
        null_count = df.filter(F.col("c").isNull()).count()
        assert null_count == 1

    def test_retail_df_row_count(self, spark):
        df = create_retail_df(spark)
        assert df.count() == len(RETAIL_ROWS)

    def test_retail_df_columns(self, spark):
        df = create_retail_df(spark)
        assert set(df.columns) == {"product", "category", "price", "quantity", "region"}

    def test_retail_df_schema_matches(self, spark):
        df = create_retail_df(spark)
        assert df.schema == RETAIL_SCHEMA

    def test_retail_df_has_nulls(self, spark):
        from pyspark.sql import functions as F

        df = create_retail_df(spark)
        price_nulls = df.filter(F.col("price").isNull()).count()
        quantity_nulls = df.filter(F.col("quantity").isNull()).count()
        assert price_nulls == 1
        assert quantity_nulls == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
