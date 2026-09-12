from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from spark_sql.runner import BackendError, LocalSparkBackend, SqlRunner

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestLocalSparkBackend:
    def test_run_returns_rows_and_columns(self: TestLocalSparkBackend, spark: SparkSession) -> None:
        backend = LocalSparkBackend(spark)
        result = backend.run("SELECT 1 AS n, 'x' AS label")
        assert backend.name == "local-spark"
        assert result.backend == "local-spark"
        assert result.columns == ["n", "label"]
        assert result.rows == [(1, "x")]

    def test_run_with_explain_captures_plan(self: TestLocalSparkBackend, spark: SparkSession) -> None:
        result = LocalSparkBackend(spark).run("SELECT 1 AS n", explain=True)
        assert result.plan is not None
        assert "Physical Plan" in result.plan

    def test_explain_rejects_unknown_mode(self: TestLocalSparkBackend, spark: SparkSession) -> None:
        with pytest.raises(ValueError, match="Unsupported EXPLAIN mode"):
            LocalSparkBackend(spark).explain("SELECT 1", mode="BOGUS")

    def test_execution_error_wrapped(self: TestLocalSparkBackend, spark: SparkSession) -> None:
        with pytest.raises(BackendError):
            LocalSparkBackend(spark).run("SELECT * FROM table_that_does_not_exist_xyz")

    def test_runner_local_constructor(self: TestLocalSparkBackend, spark: SparkSession) -> None:
        runner = SqlRunner.local(spark)
        assert runner.run("SELECT 7 AS n").scalar() == 7
