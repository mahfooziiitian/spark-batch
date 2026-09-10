---
applyTo: "sql/**/*.sql, docs/**/*.md"
---

# SQL — Spark SQL (open-source `sparksql` dialect)

`.sql` examples live under **`sql/`** (repo root), not `src/`. Tests execute them
through `src/spark_sql/_helpers.py` (`read_sql_text` / `execute_sql_file`).

## Linting

```bash
make sql               # fix + lint all SQL (parallel via SQLFLUFF_PROCESSES=0)
make sql-lint          # lint only
make sql-lint-changed  # lint only SQL changed vs HEAD
```

sqlfluff is configured for `dialect = sparksql` in `pyproject.toml`.

### Databricks-only files

SQL using Databricks-exclusive constructs (`OPTIMIZE … ZORDER`, `WHEN NOT MATCHED
BY SOURCE`, some `TABLESAMPLE` variants, …) cannot be parsed by the `sparksql`
dialect. List those files in **`.sqlfluffignore`** so linting skips them — they
stay valid on Databricks Runtime. Also label them `-- [Databricks]` in the file.

### `SELECT * FROM VALUES` inline tables (LT09 crash workaround)

sqlfluff's LT09 rule crashes on the common inline-table idiom. Use this
lint-clean layout for sample data:

```sql
SELECT -- noqa: LT09
    * --noqa
FROM
VALUES
    (1, 'Alice'),
    (2, 'Bob')
AS t (id, name);
```

## Formatting Rules

- Keywords: `UPPER`. Identifiers: `lower`.
- One clause per line. Columns indented under `SELECT`.
- Max line length: 128.
- CTE names: `snake_case`. Prefer CTEs over nested subqueries.

## File Header

```sql
-- ============================================================
-- Topic: <topic summary>
-- Dialect: Spark SQL 4 (open-source; `sparksql`)
-- Description: <what this file demonstrates>
-- ============================================================
```

## Delta & SCD

See [databricks.instructions.md](databricks.instructions.md) for Delta DML rules, `OPTIMIZE`/`ZORDER`,
and the `[Databricks]` labeling convention.

- Row hash: `md5(concat_ws('||', col1, col2, ...))`.
- Null-safe comparison: `<=>` operator.
- **SCD Type 2/6 require two-step MERGE** — one MERGE cannot expire and insert for the same key.

## Performance

- Push `WHERE` filters before joins/aggregations.
- Filter on partition columns when available.
- Use `/*+ BROADCAST(dim) */` for small dimensions (< 10 MB).
- Avoid UDFs in `WHERE` — they disable predicate pushdown.
- Prefer window functions over self-joins for row comparisons.

## Complex Types

- `TRANSFORM` / `FILTER` / `AGGREGATE` for array/map HOFs.
- `array_contains` over `explode` for membership checks.
- `LATERAL VIEW explode(...)` when one row per element is needed.

## NULL Handling

- Always `IS NULL` / `IS NOT NULL` — never `= NULL`.
- Use `COALESCE(col, default)` over `IFNULL`.
- Use `<=>` in join conditions that may contain NULLs.
