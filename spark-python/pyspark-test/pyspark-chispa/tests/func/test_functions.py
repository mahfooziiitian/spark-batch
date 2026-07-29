import pytest
from chispa import assert_approx_column_equality
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

from data_frame.func.func import clamp, divide_by_three, null_safe_divide, percentage


class TestDivideByThree:
    """Tests for the divide_by_three column function."""

    def test_basic_division(self, spark):
        data = [(1, 0.33), (2, 0.66), (3, 1.0)]
        df = spark.createDataFrame(data, ["num", "expected"]).withColumn("actual", divide_by_three(F.col("num")))
        assert_approx_column_equality(df, "actual", "expected", 0.01)

    def test_null_propagation(self, spark):
        schema = StructType([StructField("num", DoubleType()), StructField("expected", DoubleType())])
        df = spark.createDataFrame([(None, None)], schema).withColumn("actual", divide_by_three(F.col("num")))
        assert_approx_column_equality(df, "actual", "expected", 0.01)


class TestNullSafeDivide:
    """Tests for the null_safe_divide column function."""

    def test_normal_division(self, spark):
        data = [(10.0, 2.0, 5.0), (9.0, 3.0, 3.0)]
        df = spark.createDataFrame(data, ["num", "denom", "expected"]).withColumn(
            "actual", null_safe_divide(F.col("num"), F.col("denom"))
        )
        assert_approx_column_equality(df, "actual", "expected", 0.01)

    def test_zero_denominator_returns_null(self, spark):
        schema = StructType(
            [
                StructField("num", DoubleType()),
                StructField("denom", DoubleType()),
                StructField("expected", DoubleType()),
            ]
        )
        data = [(10.0, 0.0, None)]
        df = spark.createDataFrame(data, schema).withColumn("actual", null_safe_divide(F.col("num"), F.col("denom")))
        assert_approx_column_equality(df, "actual", "expected", 0.01)

    def test_null_denominator_returns_null(self, spark):
        schema = StructType(
            [
                StructField("num", DoubleType()),
                StructField("denom", DoubleType()),
                StructField("expected", DoubleType()),
            ]
        )
        data = [(10.0, None, None)]
        df = spark.createDataFrame(data, schema).withColumn("actual", null_safe_divide(F.col("num"), F.col("denom")))
        assert_approx_column_equality(df, "actual", "expected", 0.01)


class TestPercentage:
    """Tests for the percentage column function."""

    def test_basic_percentage(self, spark):
        data = [(25.0, 100.0, 25.0), (1.0, 3.0, 33.33)]
        df = spark.createDataFrame(data, ["part", "total", "expected"]).withColumn(
            "actual", percentage(F.col("part"), F.col("total"))
        )
        assert_approx_column_equality(df, "actual", "expected", 0.01)

    def test_zero_total_returns_null(self, spark):
        schema = StructType(
            [
                StructField("part", DoubleType()),
                StructField("total", DoubleType()),
                StructField("expected", DoubleType()),
            ]
        )
        data = [(10.0, 0.0, None)]
        df = spark.createDataFrame(data, schema).withColumn("actual", percentage(F.col("part"), F.col("total")))
        assert_approx_column_equality(df, "actual", "expected", 0.01)


class TestClamp:
    """Tests for the clamp column function."""

    def test_within_range_unchanged(self, spark):
        data = [(5.0, 5.0), (50.0, 50.0)]
        df = spark.createDataFrame(data, ["val", "expected"]).withColumn("actual", clamp(F.col("val"), 0.0, 100.0))
        assert_approx_column_equality(df, "actual", "expected", 0.01)

    def test_below_lower_clamped(self, spark):
        data = [(-5.0, 0.0), (-100.0, 0.0)]
        df = spark.createDataFrame(data, ["val", "expected"]).withColumn("actual", clamp(F.col("val"), 0.0, 100.0))
        assert_approx_column_equality(df, "actual", "expected", 0.01)

    def test_above_upper_clamped(self, spark):
        data = [(150.0, 100.0), (999.0, 100.0)]
        df = spark.createDataFrame(data, ["val", "expected"]).withColumn("actual", clamp(F.col("val"), 0.0, 100.0))
        assert_approx_column_equality(df, "actual", "expected", 0.01)

    def test_invalid_bounds_raises(self, spark):
        with pytest.raises(ValueError, match="must not be greater"):
            clamp(F.col("val"), 100.0, 0.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
