"""Validates SQL semantics from sql/scd/type2/scd_type2.sql. Delta MERGE is emulated with temp-view SQL."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests._helpers import assert_query_in_source, create_view

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]

RUN_DATE = "2024-06-15"
STEP_ONE_FRAGMENT = """
MERGE INTO dim_customer AS t
USING staging_customer AS s
    ON t.customer_id = s.customer_id
   AND t.is_current  = TRUE
WHEN MATCHED AND (
    t.name  <> s.name  OR
    t.email <> s.email OR
    t.city  <> s.city
) THEN
    UPDATE SET
        t.is_current   = FALSE,
        t.effective_to = CURRENT_DATE()
"""


class TestSCD2:
    @staticmethod
    def _target_schema() -> StructType:
        return StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("email", StringType(), False),
                StructField("city", StringType(), False),
                StructField("effective_from", DateType(), False),
                StructField("effective_to", DateType(), True),
                StructField("is_current", BooleanType(), False),
            ]
        )

    @staticmethod
    def _stage_schema() -> StructType:
        return StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("email", StringType(), False),
                StructField("city", StringType(), False),
            ]
        )

    @classmethod
    def _register_views(cls, spark: SparkSession) -> None:
        create_view(
            spark,
            "dim_customer",
            [
                (
                    1,
                    "Alice",
                    "alice@example.com",
                    "New York",
                    date(2023, 1, 1),
                    None,
                    True,
                ),
                (2, "Bob", "bob@example.com", "Chicago", date(2023, 1, 1), None, True),
                (
                    3,
                    "Carol",
                    "carol@example.com",
                    "Austin",
                    date(2023, 1, 1),
                    None,
                    True,
                ),
            ],
            cls._target_schema(),
        )
        create_view(
            spark,
            "staging_customer",
            [
                (1, "Alice", "alice@newemail.com", "Boston"),
                (2, "Bob", "bob@example.com", "Chicago"),
                (4, "Dave", "dave@example.com", "Seattle"),
            ],
            cls._stage_schema(),
        )

    @staticmethod
    def _run_merge(spark: SparkSession) -> DataFrame:
        return spark.sql(f"""
            WITH changed_current AS (
                SELECT
                    t.customer_id,
                    t.name,
                    t.email,
                    t.city,
                    t.effective_from,
                    DATE '{RUN_DATE}' AS effective_to,
                    FALSE AS is_current
                FROM dim_customer AS t
                INNER JOIN staging_customer AS s
                    ON t.customer_id = s.customer_id
                WHERE t.is_current = TRUE
                  AND (t.name <> s.name OR t.email <> s.email OR t.city <> s.city)
            ),
            preserved_rows AS (
                SELECT t.*
                FROM dim_customer AS t
                LEFT JOIN staging_customer AS s
                    ON t.customer_id = s.customer_id
                WHERE NOT (
                    t.is_current = TRUE
                    AND s.customer_id IS NOT NULL
                    AND (t.name <> s.name OR t.email <> s.email OR t.city <> s.city)
                )
            ),
            new_versions AS (
                SELECT
                    s.customer_id,
                    s.name,
                    s.email,
                    s.city,
                    DATE '{RUN_DATE}' AS effective_from,
                    CAST(NULL AS DATE) AS effective_to,
                    TRUE AS is_current
                FROM staging_customer AS s
                LEFT JOIN dim_customer AS t
                    ON s.customer_id = t.customer_id
                   AND t.is_current = TRUE
                WHERE t.customer_id IS NULL
                   OR t.name <> s.name
                   OR t.email <> s.email
                   OR t.city <> s.city
            )
            SELECT * FROM preserved_rows
            UNION ALL
            SELECT * FROM changed_current
            UNION ALL
            SELECT * FROM new_versions
            """)

    def test_versions_changed_and_new_rows(self: TestSCD2, spark: SparkSession) -> None:
        assert_query_in_source("sql/scd/type2/scd_type2.sql", STEP_ONE_FRAGMENT)
        self._register_views(spark)

        actual = self._run_merge(spark)
        expected = spark.createDataFrame(
            [
                (
                    1,
                    "Alice",
                    "alice@example.com",
                    "New York",
                    date(2023, 1, 1),
                    date(2024, 6, 15),
                    False,
                ),
                (
                    1,
                    "Alice",
                    "alice@newemail.com",
                    "Boston",
                    date(2024, 6, 15),
                    None,
                    True,
                ),
                (2, "Bob", "bob@example.com", "Chicago", date(2023, 1, 1), None, True),
                (
                    3,
                    "Carol",
                    "carol@example.com",
                    "Austin",
                    date(2023, 1, 1),
                    None,
                    True,
                ),
                (
                    4,
                    "Dave",
                    "dave@example.com",
                    "Seattle",
                    date(2024, 6, 15),
                    None,
                    True,
                ),
            ],
            schema=self._target_schema(),
        )
        assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)

    def test_new_record_inserted(self: TestSCD2, spark: SparkSession) -> None:
        self._register_views(spark)
        inserted = self._run_merge(spark).filter("customer_id = 4 AND is_current = TRUE").first()
        assert inserted is not None
        assert inserted.effective_from == date(2024, 6, 15)

    def test_changed_record_versioned(self: TestSCD2, spark: SparkSession) -> None:
        self._register_views(spark)
        result = self._run_merge(spark).filter("customer_id = 1")
        assert result.count() == 2
        assert result.filter("is_current = TRUE AND city = 'Boston'").count() == 1
        assert result.filter("is_current = FALSE AND effective_to = DATE '2024-06-15'").count() == 1

    def test_unchanged_record_skipped(self: TestSCD2, spark: SparkSession) -> None:
        self._register_views(spark)
        unchanged = self._run_merge(spark).filter("customer_id = 2").collect()
        assert len(unchanged) == 1
        assert unchanged[0].effective_from == date(2023, 1, 1)

    def test_idempotent_on_rerun(self: TestSCD2, spark: SparkSession) -> None:
        self._register_views(spark)
        first_run = self._run_merge(spark)
        first_run.createOrReplaceTempView("dim_customer")
        second_run = self._run_merge(spark)
        assert_df_equality(first_run, second_run, ignore_row_order=True, ignore_nullable=True)

    def test_no_duplicate_active_rows(self: TestSCD2, spark: SparkSession) -> None:
        self._register_views(spark)
        result = self._run_merge(spark)
        duplicates = result.filter("is_current = TRUE").groupBy("customer_id").count().filter("count > 1").count()
        assert duplicates == 0

    def test_point_in_time_snapshot_returns_old_version(self: TestSCD2, spark: SparkSession) -> None:
        self._register_views(spark)
        self._run_merge(spark).createOrReplaceTempView("dim_customer")
        point_in_time = spark.sql("""
            SELECT customer_id, city
            FROM dim_customer
            WHERE effective_from <= DATE '2024-06-01'
              AND (effective_to > DATE '2024-06-01' OR effective_to IS NULL)
            ORDER BY customer_id
            """)
        assert point_in_time.filter("customer_id = 1 AND city = 'New York'").count() == 1
