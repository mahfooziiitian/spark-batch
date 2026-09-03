from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestTypes:
    def test_array_query_comes_from_source(self: TestTypes, spark: SparkSession) -> None:
        execute_sql_file(
            spark,
            "sql/types/array/array.sql",
            skip_predicate=lambda statement: "ARRAY_PREPEND" in statement,
        )
        statement = statement_containing("sql/types/array/array.sql", "WHERE ARRAY_CONTAINS(tags, 'gift')")
        result = spark.sql(statement)

        assert result.count() == 2
        assert result.filter("name = 'Alice'").count() == 1

    def test_map_and_struct_queries_execute_from_source(self: TestTypes, spark: SparkSession) -> None:
        execute_sql_file(spark, "sql/types/map/map.sql")
        execute_sql_file(
            spark,
            "sql/types/struct/struct.sql",
            skip_predicate=lambda statement: "SELECT\nt.*" in statement,
        )
        map_statement = statement_containing(
            "sql/types/map/map.sql",
            "metrics['purchases'] > 0",
        )
        struct_filter = statement_containing("sql/types/struct/struct.sql", "WHERE address.country = 'USA'")
        struct_explode = statement_containing("sql/types/struct/struct.sql", "LATERAL VIEW INLINE(line_items)")

        map_result = spark.sql(map_statement)
        usa_customers = spark.sql(struct_filter)
        exploded = spark.sql(struct_explode)

        assert map_result.count() == 2
        assert usa_customers.count() == 2
        assert exploded.count() == 5

    def test_datetime_query_comes_from_source(self: TestTypes, spark: SparkSession) -> None:
        execute_sql_file(
            spark,
            "sql/types/datetime/datetime.sql",
            skip_predicate=lambda statement: "AT TIME ZONE" in statement,
        )
        statement = statement_containing(
            "sql/types/datetime/datetime.sql",
            "DATE_TRUNC('MONTH', event_ts) AS trunc_month",
        )
        result = spark.sql(statement)

        assert result.count() == 4
        assert result.filter("event_ts = TIMESTAMP '2024-03-15 08:45:00'").count() == 1

    def test_variant_queries_come_from_source(self: TestTypes, spark: SparkSession) -> None:
        execute_sql_file(
            spark,
            "sql/types/variant/variant.sql",
            skip_predicate=lambda statement: "raw_events" in statement,
        )
        basic = statement_containing("sql/types/variant/variant.sql", "payload:type::STRING AS event_type")
        exploded = statement_containing("sql/types/variant/variant.sql", "LATERAL VIEW EXPLODE((payload:items)")
        analytics = statement_containing("sql/types/variant/variant.sql", "GROUP BY payload:user.tier::STRING")

        basic_result = spark.sql(basic)
        exploded_result = spark.sql(exploded)
        analytics_result = spark.sql(analytics)

        assert basic_result.count() == 5
        assert basic_result.filter("event_id = 3 AND user_id = 99 AND event_type = 'signup'").count() == 1
        assert exploded_result.count() == 3
        assert analytics_result.count() == 2
        assert analytics_result.filter("tier = 'silver' AND purchases = 1 AND total_revenue = 89.90").count() == 1
        assert analytics_result.filter("tier = 'gold' AND total_revenue = 12.50").count() == 1
