import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession


@pytest.mark.unit
def test_range_join_tier_lookup(spark: SparkSession) -> None:
    """Non-equi join on BETWEEN maps each transaction amount to a spend tier."""
    spark.createDataFrame(
        [(1, 50.0), (2, 150.0), (3, 250.0), (4, 500.0)],
        ["txn_id", "amount"],
    ).createOrReplaceTempView("transactions")

    spark.createDataFrame(
        [("bronze", 0.0, 99.99), ("silver", 100.0, 299.99), ("gold", 300.0, 999.99)],
        ["tier", "low", "high"],
    ).createOrReplaceTempView("tiers")

    actual = spark.sql("""
        SELECT t.txn_id, t.amount, tier.tier
        FROM transactions AS t
        JOIN tiers AS tier
            ON t.amount BETWEEN tier.low AND tier.high
    """)

    expected = spark.createDataFrame(
        [
            (1, 50.0, "bronze"),
            (2, 150.0, "silver"),
            (3, 250.0, "silver"),
            (4, 500.0, "gold"),
        ],
        ["txn_id", "amount", "tier"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_overlap_detection(spark: SparkSession) -> None:
    """Non-equi join detects overlapping intervals: start_a < end_b AND start_b < end_a."""
    spark.createDataFrame(
        [(1, 1, 5), (2, 8, 12), (3, 3, 7)],
        ["id", "start_a", "end_a"],
    ).createOrReplaceTempView("intervals_a")

    spark.createDataFrame(
        [(10, 4, 9), (20, 11, 15), (30, 0, 2)],
        ["id", "start_b", "end_b"],
    ).createOrReplaceTempView("intervals_b")

    actual = spark.sql("""
        SELECT a.id AS id_a, b.id AS id_b
        FROM intervals_a AS a
        JOIN intervals_b AS b
            ON a.start_a < b.end_b
            AND b.start_b < a.end_a
    """)

    # Overlapping pairs verified manually:
    # (1,1-5) ∩ (10,4-9): 1<9 AND 4<5   ✓
    # (1,1-5) ∩ (30,0-2): 1<2 AND 0<5   ✓
    # (2,8-12)∩ (10,4-9): 8<9 AND 4<12  ✓
    # (2,8-12)∩ (20,11-15):8<15 AND 11<12 ✓
    # (3,3-7) ∩ (10,4-9): 3<9 AND 4<7   ✓
    expected = spark.createDataFrame(
        [(1, 10), (1, 30), (2, 10), (2, 20), (3, 10)],
        ["id_a", "id_b"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.unit
def test_inequality_join(spark: SparkSession) -> None:
    """Cross join filtered by a.amount > b.amount finds all higher-value order pairs."""
    spark.createDataFrame(
        [(1, 500.0), (2, 300.0), (3, 200.0), (4, 100.0)],
        ["order_id", "amount"],
    ).createOrReplaceTempView("orders_ineq")

    actual = spark.sql("""
        SELECT a.order_id AS order_a, b.order_id AS order_b
        FROM orders_ineq AS a
        CROSS JOIN orders_ineq AS b
        WHERE a.amount > b.amount
    """)

    # All 6 ordered pairs where amount(a) > amount(b):
    # 500>300, 500>200, 500>100, 300>200, 300>100, 200>100
    expected = spark.createDataFrame(
        [(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)],
        ["order_a", "order_b"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
