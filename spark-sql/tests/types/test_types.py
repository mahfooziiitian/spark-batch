from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from tests._helpers import execute_sql_file, statement_containing

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestTypes:
    def test_array_query_comes_from_source(self: TestTypes, spark: SparkSession) -> None:
        execute_sql_file(spark, "src/types/array/array.sql", skip_predicate=lambda statement: "ARRAY_PREPEND" in statement)
        statement = statement_containing("src/types/array/array.sql", "WHERE ARRAY_CONTAINS(tags, 'gift')")
        result = spark.sql(statement)

        assert result.count() == 2
        assert result.filter("name = 'Alice'").count() == 1

    def test_map_and_struct_queries_execute_from_source(self: TestTypes, spark: SparkSession) -> None:
        execute_sql_file(spark, "src/types/map/map.sql")
        execute_sql_file(
            spark,
            "src/types/struct/struct.sql",
            skip_predicate=lambda statement: "SELECT\nt.*" in statement,
        )
        map_statement = statement_containing(
            "src/types/map/map.sql",
            "metrics['purchases'] > 0",
        )
        struct_filter = statement_containing("src/types/struct/struct.sql", "WHERE address.country = 'USA'")
        struct_explode = statement_containing("src/types/struct/struct.sql", "LATERAL VIEW INLINE(line_items)")

        map_result = spark.sql(map_statement)
        usa_customers = spark.sql(struct_filter)
        exploded = spark.sql(struct_explode)

        assert map_result.count() == 2
        assert usa_customers.count() == 2
        assert exploded.count() == 5

    def test_datetime_query_comes_from_source(self: TestTypes, spark: SparkSession) -> None:
        execute_sql_file(spark, "src/types/datetime/datetime.sql", skip_predicate=lambda statement: "AT TIME ZONE" in statement)
        statement = statement_containing("src/types/datetime/datetime.sql", "DATE_TRUNC('MONTH', event_ts) AS trunc_month")
        result = spark.sql(statement)

        assert result.count() == 4
        assert result.filter("event_ts = TIMESTAMP '2024-03-15 08:45:00'").count() == 1
