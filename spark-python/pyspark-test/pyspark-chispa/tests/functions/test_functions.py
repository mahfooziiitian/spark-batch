import pytest
from chispa import assert_approx_column_equality
from pyspark.sql import functions as F

from data_frame.functions.functions import clamp, divide_by_three, null_safe_divide, percentage


class TestDivideByThree:
    """Tests for divide_by_three column function."""

    def test_positive_integers(self, spark):
        data = [(1, 0.33), (2, 0.66), (3, 1.0)]
        df = spark.createDataFrame(data, ["num", "expected"]).withColumn(
            "result", divide_by_three(F.col("num"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_null_handling(self, spark):
        from pyspark.sql.types import DoubleType, StructField, StructType

        schema = StructType([StructField("num", DoubleType()), StructField("expected", DoubleType())])
        data = [(None, None)]
        df = spark.createDataFrame(data, schema).withColumn("result", divide_by_three(F.col("num")))
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_zero(self, spark):
        data = [(0, 0.0)]
        df = spark.createDataFrame(data, ["num", "expected"]).withColumn(
            "result", divide_by_three(F.col("num"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_negative_numbers(self, spark):
        data = [(-3, -1.0), (-6, -2.0), (-9, -3.0)]
        df = spark.createDataFrame(data, ["num", "expected"]).withColumn(
            "result", divide_by_three(F.col("num"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_float_input(self, spark):
        data = [(1.5, 0.5), (4.5, 1.5), (7.5, 2.5)]
        df = spark.createDataFrame(data, ["num", "expected"]).withColumn(
            "result", divide_by_three(F.col("num"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_large_numbers(self, spark):
        data = [(3000000, 1000000.0), (9999999, 3333333.0)]
        df = spark.createDataFrame(data, ["num", "expected"]).withColumn(
            "result", divide_by_three(F.col("num"))
        )
        assert_approx_column_equality(df, "result", "expected", 1.0)


class TestNullSafeDivide:
    """Tests for null_safe_divide column function."""

    def test_normal_division(self, spark):
        data = [(10.0, 2.0, 5.0), (9.0, 3.0, 3.0)]
        df = spark.createDataFrame(data, ["num", "denom", "expected"]).withColumn(
            "result", null_safe_divide(F.col("num"), F.col("denom"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_zero_denominator_returns_null(self, spark):
        from pyspark.sql.types import DoubleType, StructField, StructType

        schema = StructType([
            StructField("num", DoubleType()),
            StructField("denom", DoubleType()),
            StructField("expected", DoubleType()),
        ])
        data = [(10.0, 0.0, None), (5.0, 0.0, None)]
        df = spark.createDataFrame(data, schema).withColumn(
            "result", null_safe_divide(F.col("num"), F.col("denom"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_null_denominator_returns_null(self, spark):
        from pyspark.sql.types import DoubleType, StructField, StructType

        schema = StructType([
            StructField("num", DoubleType()),
            StructField("denom", DoubleType()),
            StructField("expected", DoubleType()),
        ])
        data = [(10.0, None, None)]
        df = spark.createDataFrame(data, schema).withColumn(
            "result", null_safe_divide(F.col("num"), F.col("denom"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_null_numerator(self, spark):
        from pyspark.sql.types import DoubleType, StructField, StructType

        schema = StructType([
            StructField("num", DoubleType()),
            StructField("denom", DoubleType()),
            StructField("expected", DoubleType()),
        ])
        data = [(None, 5.0, None)]
        df = spark.createDataFrame(data, schema).withColumn(
            "result", null_safe_divide(F.col("num"), F.col("denom"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)


class TestPercentage:
    """Tests for percentage column function."""

    def test_basic_percentage(self, spark):
        data = [(25.0, 100.0, 25.0), (1.0, 3.0, 33.33)]
        df = spark.createDataFrame(data, ["part", "total", "expected"]).withColumn(
            "result", percentage(F.col("part"), F.col("total"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_zero_total_returns_null(self, spark):
        from pyspark.sql.types import DoubleType, StructField, StructType

        schema = StructType([
            StructField("part", DoubleType()),
            StructField("total", DoubleType()),
            StructField("expected", DoubleType()),
        ])
        data = [(10.0, 0.0, None)]
        df = spark.createDataFrame(data, schema).withColumn(
            "result", percentage(F.col("part"), F.col("total"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_hundred_percent(self, spark):
        data = [(50.0, 50.0, 100.0)]
        df = spark.createDataFrame(data, ["part", "total", "expected"]).withColumn(
            "result", percentage(F.col("part"), F.col("total"))
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_custom_decimal_places(self, spark):
        data = [(1.0, 3.0, 33.3)]
        df = spark.createDataFrame(data, ["part", "total", "expected"]).withColumn(
            "result", percentage(F.col("part"), F.col("total"), decimals=1)
        )
        assert_approx_column_equality(df, "result", "expected", 0.1)


class TestClamp:
    """Tests for clamp column function."""

    def test_values_within_range(self, spark):
        data = [(5.0, 5.0), (10.0, 10.0)]
        df = spark.createDataFrame(data, ["val", "expected"]).withColumn(
            "result", clamp(F.col("val"), 0.0, 100.0)
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_clamps_below_lower(self, spark):
        data = [(-5.0, 0.0), (-100.0, 0.0)]
        df = spark.createDataFrame(data, ["val", "expected"]).withColumn(
            "result", clamp(F.col("val"), 0.0, 100.0)
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_clamps_above_upper(self, spark):
        data = [(150.0, 100.0), (999.0, 100.0)]
        df = spark.createDataFrame(data, ["val", "expected"]).withColumn(
            "result", clamp(F.col("val"), 0.0, 100.0)
        )
        assert_approx_column_equality(df, "result", "expected", 0.01)

    def test_invalid_bounds_raises(self):
        with pytest.raises(ValueError, match="lower.*must be <= upper"):
            clamp(F.col("x"), 100.0, 0.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

