"""Validates SQL semantics from src/scd/type3/scd_type3.sql. Delta MERGE is emulated with temp-view SQL."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType
from tests._helpers import assert_query_in_source, create_view

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]

RUN_DATE = "2024-06-15"
MERGE_FRAGMENT = """
MERGE INTO dim_customer AS t
USING staging_customer AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED AND t.current_city <> s.city THEN
    UPDATE SET
        t.name = s.name,
        t.previous_city = t.current_city,
        t.current_city = s.city,
        t.current_since = CURRENT_DATE()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, current_city, previous_city, current_since)
    VALUES (s.customer_id, s.name, s.city, NULL, CURRENT_DATE())
"""


class TestSCD3:
    @staticmethod
    def _target_schema() -> StructType:
        return StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("current_city", StringType(), False),
                StructField("previous_city", StringType(), True),
                StructField("current_since", DateType(), False),
            ]
        )

    @staticmethod
    def _stage_schema() -> StructType:
        return StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("city", StringType(), False),
            ]
        )

    @classmethod
    def _register_views(cls, spark: SparkSession) -> None:
        create_view(
            spark,
            "dim_customer",
            [
                (1, "Alice", "New York", None, date(2023, 1, 1)),
                (2, "Bob", "Chicago", None, date(2023, 1, 1)),
                (3, "Carol", "Austin", None, date(2023, 1, 1)),
            ],
            cls._target_schema(),
        )
        create_view(
            spark,
            "staging_customer",
            [
                (1, "Alice", "Boston"),
                (2, "Bob", "Chicago"),
                (4, "Dave", "Seattle"),
            ],
            cls._stage_schema(),
        )

    @staticmethod
    def _run_merge(spark: SparkSession) -> DataFrame:
        return spark.sql(
            f"""
            SELECT
                COALESCE(s.customer_id, t.customer_id) AS customer_id,
                COALESCE(s.name, t.name) AS name,
                CASE
                    WHEN t.customer_id IS NULL THEN s.city
                    WHEN s.customer_id IS NULL THEN t.current_city
                    WHEN t.current_city <> s.city THEN s.city
                    ELSE t.current_city
                END AS current_city,
                CASE
                    WHEN t.customer_id IS NULL THEN CAST(NULL AS STRING)
                    WHEN s.customer_id IS NULL THEN t.previous_city
                    WHEN t.current_city <> s.city THEN t.current_city
                    ELSE t.previous_city
                END AS previous_city,
                CASE
                    WHEN t.customer_id IS NULL THEN DATE '{RUN_DATE}'
                    WHEN s.customer_id IS NULL THEN t.current_since
                    WHEN t.current_city <> s.city THEN DATE '{RUN_DATE}'
                    ELSE t.current_since
                END AS current_since
            FROM dim_customer AS t
            FULL OUTER JOIN staging_customer AS s
                ON t.customer_id = s.customer_id
            ORDER BY customer_id
            """
        )

    def test_shift_current_city_and_insert_new_customer(self: TestSCD3, spark: SparkSession) -> None:
        assert_query_in_source("src/scd/type3/scd_type3.sql", MERGE_FRAGMENT)
        self._register_views(spark)

        actual = self._run_merge(spark)
        expected = spark.createDataFrame(
            [
                (1, "Alice", "Boston", "New York", date(2024, 6, 15)),
                (2, "Bob", "Chicago", None, date(2023, 1, 1)),
                (3, "Carol", "Austin", None, date(2023, 1, 1)),
                (4, "Dave", "Seattle", None, date(2024, 6, 15)),
            ],
            schema=self._target_schema(),
        )
        assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)

    def test_new_record_inserted(self: TestSCD3, spark: SparkSession) -> None:
        self._register_views(spark)
        inserted = self._run_merge(spark).filter("customer_id = 4").first()
        assert inserted is not None
        assert inserted.previous_city is None

    def test_changed_record_moves_current_to_previous(self: TestSCD3, spark: SparkSession) -> None:
        self._register_views(spark)
        changed = self._run_merge(spark).filter("customer_id = 1").first()
        assert changed is not None
        assert changed.current_city == "Boston"
        assert changed.previous_city == "New York"

    def test_unchanged_record_skipped(self: TestSCD3, spark: SparkSession) -> None:
        self._register_views(spark)
        unchanged = self._run_merge(spark).filter("customer_id = 2").first()
        assert unchanged is not None
        assert unchanged.current_since == date(2023, 1, 1)

    def test_idempotent_on_rerun(self: TestSCD3, spark: SparkSession) -> None:
        self._register_views(spark)
        first_run = self._run_merge(spark)
        first_run.createOrReplaceTempView("dim_customer")
        second_run = self._run_merge(spark)
        assert_df_equality(first_run, second_run, ignore_row_order=True, ignore_nullable=True)

    def test_no_duplicate_customer_rows(self: TestSCD3, spark: SparkSession) -> None:
        self._register_views(spark)
        result = self._run_merge(spark)
        assert result.groupBy("customer_id").count().filter("count > 1").count() == 0
