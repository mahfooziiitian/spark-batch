import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SALES_DATA = [
    ("North", "Alice", "2024-01-01", 100),
    ("North", "Alice", "2024-01-05", 200),
    ("North", "Alice", "2024-01-10", 300),
    ("North", "Bob", "2024-01-02", 150),
    ("North", "Bob", "2024-01-06", 250),
    ("South", "Alice", "2024-01-03", 400),
    ("South", "Alice", "2024-01-07", 500),
    ("South", "Bob", "2024-01-04", 180),
    ("South", "Bob", "2024-01-08", 220),
]
SALES_COLS = ["region", "rep", "sale_date", "amount"]


@pytest.fixture(scope="module")
def sales_view(spark: SparkSession):
    spark.createDataFrame(SALES_DATA, SALES_COLS).createOrReplaceTempView("agg_sales")
    return "agg_sales"


@pytest.mark.unit
def test_running_total(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               SUM(amount) OVER (
                   PARTITION BY region, rep ORDER BY sale_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS running_total
        FROM agg_sales
        """
    )
    alice_north = (
        result.filter((F.col("region") == "North") & (F.col("rep") == "Alice"))
        .orderBy("sale_date")
        .collect()
    )
    totals = [row.running_total for row in alice_north]
    assert totals == sorted(totals), f"Running total is not monotonically increasing: {totals}"
    assert totals == [100, 300, 600]


@pytest.mark.unit
def test_partition_total_same_per_group(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               SUM(amount) OVER (PARTITION BY region) AS region_total
        FROM agg_sales
        """
    )
    north_rows = result.filter(F.col("region") == "North").collect()
    south_rows = result.filter(F.col("region") == "South").collect()

    north_totals = {row.region_total for row in north_rows}
    south_totals = {row.region_total for row in south_rows}

    assert len(north_totals) == 1, f"North region_total is not uniform: {north_totals}"
    assert len(south_totals) == 1, f"South region_total is not uniform: {south_totals}"

    # North: 100+200+300+150+250 = 1000; South: 400+500+180+220 = 1300
    assert north_totals == {1000}
    assert south_totals == {1300}


@pytest.mark.unit
def test_moving_average_3_rows(spark: SparkSession):
    seq_data = [("A", 1, 10), ("A", 2, 20), ("A", 3, 30), ("A", 4, 40), ("A", 5, 50)]
    spark.createDataFrame(seq_data, ["grp", "pos", "val"]).createOrReplaceTempView(
        "seq_data"
    )
    result = spark.sql(
        """
        SELECT grp, pos, val,
               AVG(val) OVER (
                   PARTITION BY grp ORDER BY pos
                   ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
               ) AS moving_avg
        FROM seq_data
        """
    )
    rows = {row.pos: row.moving_avg for row in result.collect()}
    # pos=1: avg(10,20)=15.0; pos=2: avg(10,20,30)=20.0; pos=3: avg(20,30,40)=30.0
    # pos=4: avg(30,40,50)=40.0; pos=5: avg(40,50)=45.0
    assert rows[1] == 15.0
    assert rows[2] == 20.0
    assert rows[3] == 30.0
    assert rows[4] == 40.0
    assert rows[5] == 45.0


@pytest.mark.unit
def test_cumulative_pct(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               SUM(amount) OVER (
                   PARTITION BY region, rep ORDER BY sale_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS running_sum,
               SUM(amount) OVER (PARTITION BY region, rep) AS total_sum,
               SUM(amount) OVER (
                   PARTITION BY region, rep ORDER BY sale_date
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) / SUM(amount) OVER (PARTITION BY region, rep) AS cumulative_pct
        FROM agg_sales
        """
    )
    rows = result.collect()
    for row in rows:
        assert 0.0 < row.cumulative_pct <= 1.0, (
            f"cumulative_pct out of range for {row.region}/{row.rep}/{row.sale_date}: {row.cumulative_pct}"
        )

    # Last row per partition must equal 1.0
    last_rows = spark.sql(
        """
        SELECT region, rep, cumulative_pct
        FROM (
            SELECT region, rep, sale_date, cumulative_pct,
                   ROW_NUMBER() OVER (PARTITION BY region, rep ORDER BY sale_date DESC) AS rn
            FROM (
                SELECT region, rep, sale_date,
                       SUM(amount) OVER (
                           PARTITION BY region, rep ORDER BY sale_date
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) / SUM(amount) OVER (PARTITION BY region, rep) AS cumulative_pct
                FROM agg_sales
            )
        )
        WHERE rn = 1
        """
    ).collect()
    for row in last_rows:
        assert row.cumulative_pct == 1.0, (
            f"Last row cumulative_pct != 1.0 for {row.region}/{row.rep}: {row.cumulative_pct}"
        )


@pytest.mark.unit
def test_partition_count(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date,
               COUNT(*) OVER (PARTITION BY region) AS region_count
        FROM agg_sales
        """
    )
    north_counts = {row.region_count for row in result.filter(F.col("region") == "North").collect()}
    south_counts = {row.region_count for row in result.filter(F.col("region") == "South").collect()}

    # North has 5 rows (Alice×3 + Bob×2), South has 4 rows (Alice×2 + Bob×2)
    assert north_counts == {5}
    assert south_counts == {4}
