"""Validates SQL semantics from sql/scd/type1/scd_type1.sql. Delta MERGE is emulated with temp-view SQL."""

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
CONDITIONAL_FRAGMENT = """
WHEN MATCHED AND (
    t.name <> s.name
    OR t.email <> s.email
    OR t.city <> s.city
) THEN
    UPDATE SET
        t.name = s.name,
        t.email = s.email,
        t.city = s.city,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, updated_at)
    VALUES (s.customer_id, s.name, s.email, s.city, CURRENT_TIMESTAMP())
"""


class TestSCD1:
    @staticmethod
    def _target_schema() -> StructType:
        return StructType(
            [
                StructField("customer_id", IntegerType(), False),
                StructField("name", StringType(), False),
                StructField("email", StringType(), False),
                StructField("city", StringType(), False),
                StructField("updated_at", DateType(), False),
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
                (1, "Alice", "alice@example.com", "New York", date(2024, 1, 1)),
                (2, "Bob", "bob@example.com", "Chicago", date(2024, 1, 1)),
                (3, "Carol", "carol@example.com", "Austin", date(2024, 1, 1)),
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
            SELECT
                COALESCE(s.customer_id, t.customer_id) AS customer_id,
                CASE
                    WHEN t.customer_id IS NULL THEN s.name
                    WHEN s.customer_id IS NULL THEN t.name
                    WHEN t.name <> s.name OR t.email <> s.email OR t.city <> s.city THEN s.name
                    ELSE t.name
                END AS name,
                CASE
                    WHEN t.customer_id IS NULL THEN s.email
                    WHEN s.customer_id IS NULL THEN t.email
                    WHEN t.name <> s.name OR t.email <> s.email OR t.city <> s.city THEN s.email
                    ELSE t.email
                END AS email,
                CASE
                    WHEN t.customer_id IS NULL THEN s.city
                    WHEN s.customer_id IS NULL THEN t.city
                    WHEN t.name <> s.name OR t.email <> s.email OR t.city <> s.city THEN s.city
                    ELSE t.city
                END AS city,
                CASE
                    WHEN t.customer_id IS NULL THEN DATE '{RUN_DATE}'
                    WHEN s.customer_id IS NULL THEN t.updated_at
                    WHEN t.name <> s.name OR t.email <> s.email OR t.city <> s.city THEN DATE '{RUN_DATE}'
                    ELSE t.updated_at
                END AS updated_at
            FROM dim_customer AS t
            FULL OUTER JOIN staging_customer AS s
                ON t.customer_id = s.customer_id
            ORDER BY customer_id
            """)

    def test_merge_applies_insert_update_and_skip(self: TestSCD1, spark: SparkSession) -> None:
        assert_query_in_source("sql/scd/type1/scd_type1.sql", CONDITIONAL_FRAGMENT)
        self._register_views(spark)

        actual = self._run_merge(spark)
        expected = spark.createDataFrame(
            [
                (1, "Alice", "alice@newemail.com", "Boston", date(2024, 6, 15)),
                (2, "Bob", "bob@example.com", "Chicago", date(2024, 1, 1)),
                (3, "Carol", "carol@example.com", "Austin", date(2024, 1, 1)),
                (4, "Dave", "dave@example.com", "Seattle", date(2024, 6, 15)),
            ],
            schema=self._target_schema(),
        )

        assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)

    def test_new_record_inserted(self: TestSCD1, spark: SparkSession) -> None:
        self._register_views(spark)
        inserted = self._run_merge(spark).filter("customer_id = 4").first()
        assert inserted is not None
        assert inserted.name == "Dave"
        assert inserted.updated_at == date(2024, 6, 15)

    def test_changed_record_updated(self: TestSCD1, spark: SparkSession) -> None:
        self._register_views(spark)
        updated = self._run_merge(spark).filter("customer_id = 1").first()
        assert updated is not None
        assert updated.email == "alice@newemail.com"
        assert updated.city == "Boston"

    def test_unchanged_record_skipped(self: TestSCD1, spark: SparkSession) -> None:
        self._register_views(spark)
        unchanged = self._run_merge(spark).filter("customer_id = 2").first()
        assert unchanged is not None
        assert unchanged.email == "bob@example.com"
        assert unchanged.updated_at == date(2024, 1, 1)

    def test_idempotent_on_rerun(self: TestSCD1, spark: SparkSession) -> None:
        self._register_views(spark)
        first_run = self._run_merge(spark)
        first_run.createOrReplaceTempView("dim_customer")
        second_run = self._run_merge(spark)
        assert_df_equality(first_run, second_run, ignore_row_order=True, ignore_nullable=True)

    def test_no_duplicate_rows_after_merge(self: TestSCD1, spark: SparkSession) -> None:
        self._register_views(spark)
        result = self._run_merge(spark)
        duplicate_count = result.groupBy("customer_id").count().filter("count > 1").count()
        assert duplicate_count == 0
