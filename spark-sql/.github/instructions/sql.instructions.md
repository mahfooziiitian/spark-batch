---
applyTo: "src/**/*.sql"
---

# SQL — Spark SQL (Databricks Dialect)

## Linting

```bash
uv run task sql        # fix + lint
uv run task sql_lint   # lint only
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
-- Dialect: Databricks / Spark SQL 3.5
-- Description: <what this file demonstrates>
-- ============================================================
```

## Delta DML

- **Deduplicate source before MERGE** — Delta errors on multiple source matches.
- Use `MERGE INTO` for upserts over separate INSERT + UPDATE.

## SCD Conventions

- Row hash: `md5(concat_ws('||', col1, col2, ...))`.
- Null-safe comparison: `<=>` operator.
- **SCD Type 2/6 require two-step MERGE** — one MERGE cannot expire and insert for the same key.

## Performance

- Push `WHERE` filters before joins/aggregations.
- Filter on partition columns when available.
- Use `/*+ BROADCAST(dim) */` for small dimensions (< 10 MB).
- Avoid UDFs in `WHERE` — they disable predicate pushdown.
- `OPTIMIZE ... ZORDER BY (col)` after bulk writes.

## Complex Types

- `TRANSFORM` / `FILTER` / `AGGREGATE` for array/map HOFs.
- `array_contains` over `explode` for membership checks.
- `LATERAL VIEW explode(...)` when one row per element is needed.

## NULL Handling

- Always `IS NULL` / `IS NOT NULL` — never `= NULL`.
- Use `COALESCE(col, default)` over `IFNULL`.
- Use `<=>` in join conditions that may contain NULLs.
