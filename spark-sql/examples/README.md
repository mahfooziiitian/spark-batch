# Examples — running SQL with `spark_sql.runner`

Runnable scripts that drive the `src/spark_sql/runner` library (runner /
examiner / validator) against **local Spark** and, optionally, a **Databricks SQL
warehouse**. They exercise real files under `sql/` the same way the pytest suite does.

## Run

From the repository root:

```bash
uv run python examples/run_statement.py
uv run python examples/run_sql_file.py                       # defaults to sql/nulls/nulls.sql
uv run python examples/run_sql_file.py sql/window/ranking/ranking.sql
uv run python examples/examine_plan.py
uv run python examples/validate_results.py
uv run python examples/databricks_backend.py                # no-op unless Databricks env is set
```

## Scripts

| Script | Shows |
|--------|-------|
| `run_statement.py` | Run one statement, `summarize()` it, `expect(...)` assertions, capture the physical plan. |
| `run_sql_file.py` | Execute a whole `.sql` file (`SqlRunner.run_file`, `queries_only=True`) and print each result. |
| `examine_plan.py` | `SqlExaminer.plan_summary` + `expect_plan(...)` — assert broadcast join / shuffle behaviour. |
| `validate_results.py` | Validate a real `sql/` example and catch a failing `ValidationError`. |
| `databricks_backend.py` | Same API against `SqlRunner.databricks(...)`; safe no-op until configured. |
| `_session.py` | Shared local `SparkSession` builder (not an example itself). |

## Configuration

| Variable | Effect |
|----------|--------|
| `DATA_HOME` | Base dir for `spark-warehouse` / `metastore_db` / `derby.log` (default `/tmp/spark-sql-examples`). |
| `SPARK_MASTER` | Spark master for local runs (default `local[*]`). |
| `APP_ENV` | Selects the `.env.<target>` overlay `databricks_backend.py` loads (default `dev`). |
| `DATABRICKS_CONFIG_PROFILE` *or* `DATABRICKS_HOST` + `DATABRICKS_TOKEN` | Databricks auth. |
| `DATABRICKS_WAREHOUSE_NAME` *or* `DATABRICKS_WAREHOUSE_ID` | Target SQL warehouse. |

`databricks_backend.py` resolves the Databricks variables through the repo's own
`load_env_layers()` — it loads `.env` then `.env.<APP_ENV>` (same layering as
`make`), so values in `.env.dev` (profile, host, warehouse name) are picked up
automatically. Local examples only use `DATA_HOME` / `SPARK_MASTER`.

See [`../src/spark_sql/runner/README.md`](../src/spark_sql/runner/README.md) for the
full library reference.
