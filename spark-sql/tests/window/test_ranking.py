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
    spark.createDataFrame(SALES_DATA, SALES_COLS).createOrReplaceTempView("sales")
    return "sales"


@pytest.mark.unit
def test_row_number_unique_per_partition(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount,
               ROW_NUMBER() OVER (PARTITION BY region, rep ORDER BY sale_date) AS rn
        FROM sales
        """
    )
    # Collect partition groups and assert rn values are unique (1,2,3,...) within each
    rows = result.collect()
    from collections import defaultdict

    groups: dict = defaultdict(list)
    for row in rows:
        groups[(row.region, row.rep)].append(row.rn)

    for key, rns in groups.items():
        assert sorted(rns) == list(range(1, len(rns) + 1)), (
            f"ROW_NUMBER not unique/sequential for partition {key}: {rns}"
        )


@pytest.mark.unit
def test_rank_ties_produce_gaps(spark: SparkSession):
    tied_data = [("A", 100), ("A", 100), ("A", 200)]
    spark.createDataFrame(tied_data, ["grp", "score"]).createOrReplaceTempView(
        "tied_scores"
    )
    result = spark.sql(
        "SELECT grp, score, RANK() OVER (PARTITION BY grp ORDER BY score) AS rnk FROM tied_scores"
    )
    ranks = sorted([row.rnk for row in result.collect()])
    assert ranks == [1, 1, 3], f"Expected [1,1,3] with gap, got {ranks}"


@pytest.mark.unit
def test_dense_rank_no_gaps(spark: SparkSession):
    tied_data = [("A", 100), ("A", 100), ("A", 200)]
    spark.createDataFrame(tied_data, ["grp", "score"]).createOrReplaceTempView(
        "tied_scores_dense"
    )
    result = spark.sql(
        "SELECT grp, score, DENSE_RANK() OVER (PARTITION BY grp ORDER BY score) AS drnk "
        "FROM tied_scores_dense"
    )
    ranks = sorted([row.drnk for row in result.collect()])
    assert ranks == [1, 1, 2], f"Expected [1,1,2] with no gap, got {ranks}"


@pytest.mark.unit
def test_ntile_distributes_evenly(spark: SparkSession):
    even_data = [("A", 1), ("A", 2), ("A", 3), ("A", 4)]
    spark.createDataFrame(even_data, ["grp", "val"]).createOrReplaceTempView(
        "even_rows"
    )
    result = spark.sql(
        "SELECT grp, val, NTILE(2) OVER (PARTITION BY grp ORDER BY val) AS bucket FROM even_rows"
    )
    from collections import Counter

    bucket_counts = Counter(row.bucket for row in result.collect())
    assert bucket_counts[1] == 2 and bucket_counts[2] == 2, (
        f"Expected 2 rows per bucket, got {dict(bucket_counts)}"
    )


@pytest.mark.unit
def test_percent_rank_first_row_zero(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date,
               PERCENT_RANK() OVER (PARTITION BY region, rep ORDER BY sale_date) AS pct_rnk
        FROM sales
        """
    )
    first_rows = result.filter(F.col("pct_rnk") == 0.0).collect()
    # Each (region, rep) partition must have exactly one row with pct_rnk = 0.0
    from collections import Counter

    partition_zeros = Counter((row.region, row.rep) for row in first_rows)
    for key, count in partition_zeros.items():
        assert count == 1, f"Partition {key} has {count} rows with pct_rnk=0.0"

    # Alice-North: 3 rows → second row = 0.5, third = 1.0
    ordered = (
        result.filter((F.col("region") == "North") & (F.col("rep") == "Alice"))
        .orderBy("sale_date")
        .collect()
    )
    assert ordered[0].pct_rnk == 0.0


@pytest.mark.unit
def test_top_n_per_group(spark: SparkSession, sales_view: str):
    result = spark.sql(
        """
        SELECT region, rep, sale_date, amount
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY region, rep ORDER BY amount DESC) AS rn
            FROM sales
        )
        WHERE rn = 1
        """
    )
    rows = result.collect()
    # One row per (region, rep) partition
    partitions = [(row.region, row.rep) for row in rows]
    assert len(partitions) == len(set(partitions)), "More than one top row per partition"

    expected = spark.createDataFrame(
        [
            ("North", "Alice", "2024-01-10", 300),
            ("North", "Bob", "2024-01-06", 250),
            ("South", "Alice", "2024-01-07", 500),
            ("South", "Bob", "2024-01-08", 220),
        ],
        SALES_COLS,
    )
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_deduplication_with_row_number(spark: SparkSession):
    dup_data = [
        ("cust1", "2024-01-05", 100),
        ("cust1", "2024-01-05", 100),  # exact duplicate
        ("cust2", "2024-01-03", 200),
    ]
    spark.createDataFrame(dup_data, ["customer_id", "txn_date", "amount"]).createOrReplaceTempView(
        "dup_txns"
    )
    result = spark.sql(
        """
        SELECT customer_id, txn_date, amount
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id, txn_date, amount ORDER BY txn_date) AS rn
            FROM dup_txns
        )
        WHERE rn = 1
        """
    )
    expected = spark.createDataFrame(
        [("cust1", "2024-01-05", 100), ("cust2", "2024-01-03", 200)],
        ["customer_id", "txn_date", "amount"],
    )
    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)
