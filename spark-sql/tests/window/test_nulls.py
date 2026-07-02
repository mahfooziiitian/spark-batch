import pytest
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def null_df(spark: SparkSession):
    """Dataset with NULL amounts interspersed for IGNORE NULLS tests."""
    null_data = [
        ("A", "2024-01-01", None),
        ("A", "2024-01-02", 100),
        ("A", "2024-01-03", None),
        ("A", "2024-01-04", 200),
        ("A", "2024-01-05", None),
    ]
    return spark.createDataFrame(null_data, ["grp", "dt", "amount"])


@pytest.mark.unit
def test_first_value_ignore_nulls(spark: SparkSession, null_df):
    """first() with ignorenulls=True skips null amounts and returns first non-null."""
    w = Window.partitionBy("grp").orderBy("dt").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    result = null_df.withColumn("first_non_null", F.first(F.col("amount"), ignorenulls=True).over(w))
    rows = result.collect()
    for row in rows:
        assert row.first_non_null == 100, f"first(ignorenulls=True) should be 100 for row {row.dt}, got {row.first_non_null}"


@pytest.mark.unit
def test_last_value_ignore_nulls_forward_fill(spark: SparkSession, null_df):
    """last() with ignorenulls=True over a running frame forward-fills non-null values."""
    w = Window.partitionBy("grp").orderBy("dt").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    result = null_df.withColumn("filled_amount", F.last(F.col("amount"), ignorenulls=True).over(w))
    rows = result.orderBy("dt").collect()
    filled = [row.filled_amount for row in rows]
    # dt1=NULL→None, dt2=100→100, dt3=NULL→100(filled), dt4=200→200, dt5=NULL→200(filled)
    assert filled == [None, 100, 100, 200, 200]


@pytest.mark.unit
def test_lag_ignore_nulls(spark: SparkSession, null_df):
    """last() with ignorenulls=True over a strictly-preceding frame simulates LAG IGNORE NULLS."""
    # LAG IGNORE NULLS = last non-null value strictly before the current row
    w = Window.partitionBy("grp").orderBy("dt").rowsBetween(Window.unboundedPreceding, -1)
    result = null_df.withColumn("prev_non_null", F.last(F.col("amount"), ignorenulls=True).over(w))
    rows = result.orderBy("dt").collect()
    prev = [row.prev_non_null for row in rows]
    # dt1: no prior rows → None
    # dt2(100): prior rows = [null] → no non-null → None
    # dt3(null): prior rows = [null, 100] → last non-null = 100
    # dt4(200): prior rows = [null, 100, null] → last non-null = 100
    # dt5(null): prior rows = [null, 100, null, 200] → last non-null = 200
    assert prev[0] is None
    assert prev[1] is None
    assert prev[2] == 100
    assert prev[3] == 100
    assert prev[4] == 200
