"""Backend-agnostic SQL runner, examiner, and validator.

This package provides a small library for **executing** SQL, **examining** how it
runs, and **validating** its results and plans — against either a local
:class:`~pyspark.sql.SparkSession` or a Databricks SQL warehouse (via the
Databricks SDK Statement Execution API, the same path the ``dbx_mcp`` server uses).

Typical usage::

    from pyspark.sql import SparkSession
    from spark_sql.runner import SqlRunner, expect

    runner = SqlRunner.local(SparkSession.builder.getOrCreate())
    result = runner.run("SELECT 1 AS n")
    expect(result).non_empty().scalar(1)

    # Databricks (auth resolved from a CLI profile or host/token):
    runner = SqlRunner.databricks(profile="my-workspace")

The package has no import-time side effects.
"""

from __future__ import annotations

from spark_sql.runner.backends import (
    DatabricksBackend,
    LocalSparkBackend,
    SqlBackend,
)
from spark_sql.runner.examiner import (
    PlanSummary,
    SqlExaminer,
    analyze_plan,
    summarize,
)
from spark_sql.runner.exceptions import (
    BackendError,
    RunnerError,
    ValidationError,
)
from spark_sql.runner.result import SqlResult
from spark_sql.runner.runner import SqlRunner
from spark_sql.runner.validator import (
    PlanExpectation,
    ResultExpectation,
    expect,
    expect_broadcast_join,
    expect_columns,
    expect_contains_row,
    expect_empty,
    expect_min_row_count,
    expect_no_cartesian,
    expect_no_shuffle,
    expect_non_empty,
    expect_plan,
    expect_plan_contains,
    expect_row_count,
    expect_rows,
    expect_scalar,
    expect_shuffle_count,
)

__all__ = [
    "BackendError",
    "DatabricksBackend",
    "LocalSparkBackend",
    "PlanExpectation",
    "PlanSummary",
    "ResultExpectation",
    "RunnerError",
    "SqlBackend",
    "SqlExaminer",
    "SqlResult",
    "SqlRunner",
    "ValidationError",
    "analyze_plan",
    "expect",
    "expect_broadcast_join",
    "expect_columns",
    "expect_contains_row",
    "expect_empty",
    "expect_min_row_count",
    "expect_no_cartesian",
    "expect_no_shuffle",
    "expect_non_empty",
    "expect_plan",
    "expect_plan_contains",
    "expect_row_count",
    "expect_rows",
    "expect_scalar",
    "expect_shuffle_count",
    "summarize",
]
