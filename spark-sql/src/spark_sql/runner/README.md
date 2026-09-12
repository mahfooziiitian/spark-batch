# `spark_sql.runner` — SQL runner, examiner & validator

A small, backend-agnostic library for **running** SQL, **examining** how it
executes, and **validating** its results and plans — against either a local
`SparkSession` or a Databricks SQL warehouse.

The same runner/examiner/validator code drives both paths, so an example under
`sql/` can be validated locally in the pytest suite *and* checked on Databricks
with no call-site changes.

```
┌────────────┐   run/explain    ┌────────────┐
│  SqlRunner │ ───────────────► │ SqlBackend │  LocalSparkBackend | DatabricksBackend
└────────────┘                  └────────────┘
      │  SqlResult (rows/columns/plan)
      ▼
┌────────────┐   analyze_plan   ┌─────────────┐   expect(...)      ┌───────────┐
│  examiner  │ ───────────────► │ PlanSummary │ ─────────────────► │ validator │
└────────────┘                  └─────────────┘                    └───────────┘
```

## Components

| Module | Purpose |
|--------|---------|
| `result.py` | `SqlResult` — immutable, backend-neutral snapshot of one statement (columns, rows, backend, optional plan). |
| `backends.py` | `SqlBackend` ABC + `LocalSparkBackend` (injected `SparkSession`) + `DatabricksBackend` (SDK Statement Execution API). |
| `runner.py` | `SqlRunner` — run single statements, run whole `.sql` files, capture `EXPLAIN` plans. |
| `examiner.py` | `analyze_plan`, `PlanSummary`, `summarize`, `SqlExaminer` — count shuffles/joins/scans, describe result shape. |
| `validator.py` | Functional `expect_*` assertions + fluent `expect(...)` / `expect_plan(...)` raising `ValidationError`. |
| `exceptions.py` | `RunnerError` → `BackendError`, `ValidationError` (also an `AssertionError`). |

## Local Spark

```python
from pyspark.sql import SparkSession
from spark_sql.runner import SqlRunner, SqlExaminer, expect, expect_plan

spark = SparkSession.builder.master("local[*]").getOrCreate()
runner = SqlRunner.local(spark)

# run a statement
result = runner.run("SELECT 1 AS n")
expect(result).non_empty().scalar(1)

# run every statement in a repo .sql file (comment-aware split)
results = runner.run_file("sql/nulls/nulls.sql", queries_only=True)

# examine the physical plan and assert on execution
summary = SqlExaminer(runner).plan_summary("SELECT a.id FROM t1 a JOIN t2 b ON a.id = b.id")
expect_plan(summary).broadcast_join().no_cartesian()
```

## Databricks warehouse

Auth is resolved by `spark_sql.util.cli_env.get_workspace_client`
(profile → host/token → SDK default). The warehouse comes from
`warehouse_id` / `warehouse_name` args or `DATABRICKS_WAREHOUSE_ID` /
`DATABRICKS_WAREHOUSE_NAME`.

```python
from spark_sql.runner import SqlRunner, expect

runner = SqlRunner.databricks(profile="my-workspace", warehouse_name="Shared Endpoint")

result = runner.run("SELECT count(*) AS c FROM samples.nyctaxi.trips")
expect(result).non_empty()
```

`DatabricksBackend` uses `WorkspaceClient.statement_execution.execute_statement`
— the same read path as the `dbx_mcp` MCP server — so results are parsed from
`response.manifest.schema.columns[].name` and `response.result.data_array`.

## Validation reference

**Results** (`expect(result)` / `expect_*`): `non_empty`, `empty`,
`row_count`, `min_rows`, `columns(..., exact=)`, `scalar`, `rows(..., ignore_order=)`,
`contains_row`.

**Plans** (`expect_plan(summary)` / `expect_*`): `no_shuffle`,
`shuffle_count`, `broadcast_join`, `no_cartesian`, `contains(token)`.

All failures raise `ValidationError`, which subclasses `AssertionError`, so they
read naturally inside pytest.

## Design notes

- **No import-time side effects** and **no `SparkSession` created inside the
  library** — the session is always injected (project convention).
- Plan analysis is **textual** (Spark operator node names), keeping the examiner
  identical across the local and Databricks backends.
- Runtime deps only: `pyspark` and `databricks-sdk` (already required); the SDK
  Statement Execution path covers the "SQL connector" need without a new dependency.
