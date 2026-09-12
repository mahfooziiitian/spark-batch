from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from spark_sql.runner import SqlRunner, analyze_plan, summarize
from spark_sql.runner.examiner import SqlExaminer

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]

_JOIN_SQL = """
SELECT a.id, b.label
FROM (VALUES (1), (2), (3)) AS a(id)
JOIN (VALUES (1, 'x'), (2, 'y')) AS b(id, label)
ON a.id = b.id
"""

_GROUP_SQL = "SELECT id, count(*) AS c FROM (VALUES (1), (1), (2)) AS t(id) GROUP BY id"


class TestAnalyzePlan:
    def test_counts_from_static_plan(self: TestAnalyzePlan) -> None:
        plan = (
            "== Physical Plan ==\n"
            "BroadcastHashJoin [id], [id], Inner\n"
            "Exchange hashpartitioning\n"
            "Scan ExistingRDD\n"
            "Scan ExistingRDD\n"
        )
        summary = analyze_plan(plan)
        assert summary.exchanges == 1
        assert summary.has_shuffle
        assert summary.scans == 2
        assert summary.join_strategies == ["broadcast"]
        assert summary.uses_broadcast_join()
        assert not summary.uses_sort_merge_join()
        assert summary.join_count == 1
        assert summary.contains("BroadcastHashJoin")

    def test_broadcast_join_plan_from_spark(self: TestAnalyzePlan, spark: SparkSession) -> None:
        summary = SqlExaminer(SqlRunner.local(spark)).plan_summary(_JOIN_SQL)
        assert summary.uses_broadcast_join()

    def test_group_by_has_exchange(self: TestAnalyzePlan, spark: SparkSession) -> None:
        summary = SqlExaminer(SqlRunner.local(spark)).plan_summary(_GROUP_SQL)
        assert summary.has_shuffle


class TestSummarize:
    def test_summary_dict(self: TestSummarize, spark: SparkSession) -> None:
        result = SqlRunner.local(spark).run("SELECT 1 AS n, 2 AS m")
        summary = summarize(result)
        assert summary["backend"] == "local-spark"
        assert summary["row_count"] == 1
        assert summary["column_count"] == 2
        assert summary["columns"] == ["n", "m"]
        assert summary["is_empty"] is False
        assert summary["has_plan"] is False
