from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING

import pytest
from chispa.dataframe_comparer import assert_df_equality
from tests._helpers import create_view, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestDML:
    @staticmethod
    def _register_views(spark: SparkSession) -> None:
        create_view(
            spark,
            "raw_orders",
            [
                (1, "Alice", 250.0, "EAST", "2024-01-10"),
                (2, "Bob", 900.0, "WEST", "2024-01-11"),
                (3, "Carol", 75.0, "EAST", "2024-01-12"),
                (4, "Dave", 1200.0, "WEST", "2024-01-13"),
                (5, "Eve", 430.0, "EAST", "2024-01-14"),
            ],
            "order_id int, customer string, amount double, region string, order_date string",
        )
        create_view(
            spark,
            "orders",
            [
                (1, "Alice", 250.0, "EAST", "shipped"),
                (2, "Bob", 900.0, "WEST", "cancelled"),
                (6, "Frank", 100.0, "NORTH", "delivered"),
                (7, "Grace", 520.0, "NORTH", "shipped"),
                (99, "Ghost", 0.0, "EAST", "cancelled"),
            ],
            "order_id int, customer string, amount double, region string, status string",
        )
        create_view(
            spark,
            "valid_customers",
            [("Alice",), ("Bob",), ("Carol",), ("Dave",), ("Eve",), ("Frank",), ("Grace",), ("Hank",)],
            "customer_id string",
        )
        create_view(
            spark,
            "employees",
            [
                (1, "Alice", "Engineering", 85000.0, "L3"),
                (2, "Bob", "Engineering", 72000.0, "L2"),
                (3, "Carol", "Marketing", 65000.0, "L2"),
                (4, "Dave", "Marketing", 58000.0, "L1"),
                (5, "Eve", "Finance", 95000.0, "L4"),
                (6, "Frank", "Finance", 88000.0, "L3"),
            ],
            "emp_id int, name string, department string, salary double, level string",
        )
        create_view(
            spark,
            "raw_feed",
            [
                (1, "Widget A", 11.99, 120, datetime(2024, 3, 1, 9, 0, 0)),
                (1, "Widget A", 11.99, 125, datetime(2024, 3, 1, 10, 0, 0)),
                (6, "Sprocket", 7.49, 75, datetime(2024, 3, 1, 8, 0, 0)),
            ],
            "product_id int, name string, price double, stock int, arrived_at timestamp",
        )

    def test_insert_query_logic_comes_from_source(self: TestDML, spark: SparkSession) -> None:
        self._register_views(spark)
        source_statement = statement_containing("src/dml/insert.sql", "WHERE amount >= 500.00")
        select_statement = source_statement[source_statement.index("SELECT") :]

        actual = spark.sql(select_statement)
        expected = spark.createDataFrame(
            [(2, "Bob", 900.0, "WEST", "2024-01-11"), (4, "Dave", 1200.0, "WEST", "2024-01-13")],
            "order_id int, customer string, amount double, region string, order_date string",
        )

        assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)

    def test_delete_query_logic_comes_from_source(self: TestDML, spark: SparkSession) -> None:
        self._register_views(spark)
        delete_statement = statement_containing("src/dml/delete.sql", "DELETE FROM orders\nWHERE NOT EXISTS")
        where_clause = delete_statement.split("WHERE", maxsplit=1)[1]
        actual = spark.sql(f"SELECT order_id, customer FROM orders WHERE {where_clause}")

        assert actual.count() == 1
        orphan_row = actual.first()
        assert orphan_row is not None and orphan_row.customer == "Ghost"

    def test_update_query_logic_comes_from_source(self: TestDML, spark: SparkSession) -> None:
        self._register_views(spark)
        update_statement = statement_containing("src/dml/update.sql", "SET salary = salary * CASE")
        match = re.search(r"SET salary = salary \* (CASE.*?END)", update_statement, re.DOTALL)
        assert match is not None

        actual = spark.sql(
            f"SELECT emp_id, ROUND(salary * {match.group(1)}, 2) AS new_salary FROM employees ORDER BY emp_id"
        )

        assert actual.count() == 6
        assert actual.filter("emp_id = 4 AND new_salary = 60900.0").count() == 1

    def test_merge_deduplication_logic_comes_from_source(self: TestDML, spark: SparkSession) -> None:
        self._register_views(spark)
        merge_statement = statement_containing("src/dml/merge.sql", "WITH deduped AS")
        cte = merge_statement[: merge_statement.index("MERGE INTO")]
        actual = spark.sql(f"{cte} SELECT product_id, name, stock FROM deduped")
        expected = spark.createDataFrame(
            [(1, "Widget A", 125), (6, "Sprocket", 75)],
            "product_id int, name string, stock int",
        )

        assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
