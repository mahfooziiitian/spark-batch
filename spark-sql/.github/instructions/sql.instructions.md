---
applyTo: "src/**/*.sql"
---

# SQL Instructions — Spark SQL (Databricks)

## Dialect & Linter

- **Dialect**: Databricks (`spark.sql.ansi.enabled = true` implied)
- **Linter / Formatter**: SQLFluff — config lives in `[tool.sqlfluff.*]` sections of `pyproject.toml`
- Run lint: `uv run task sql_lint`
- Auto-fix: `uv run task sql_format`
- Combined: `uv run task sql`

## SQLFluff Rules (from pyproject.toml)

| Rule | Setting |
|------|---------|
| Dialect | `databricks` |
| Keyword capitalisation | `UPPER` |
| Indented joins | `false` |
| Indented USING/ON | `true` |
| File extensions | `.sql`, `.sql.j2`, `.dml`, `.ddl` |

## Formatting Style

```sql
-- ✅ CORRECT — keywords UPPER, indented columns, newline per clause
SELECT
    order_id,
    customer_id,
    SUM(amount) AS total_amount
FROM orders AS o
JOIN customers AS c
    ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY
    order_id,
    customer_id
HAVING SUM(amount) > 100
ORDER BY total_amount DESC;

-- ❌ WRONG — mixed case, no indentation, everything on one line
select order_id, sum(amount) from orders where order_date>='2024-01-01' group by order_id;
```

## CTEs

- Use CTEs (`WITH`) over subqueries for readability
- One CTE per logical transformation step
- CTE names use `snake_case`

```sql
WITH filtered_orders AS (
    SELECT *
    FROM orders
    WHERE order_date >= '2024-01-01'
),
aggregated AS (
    SELECT
        customer_id,
        SUM(amount) AS total
    FROM filtered_orders
    GROUP BY customer_id
)
SELECT * FROM aggregated WHERE total > 500;
```

## Delta Lake DML

- `UPDATE`, `DELETE`, `MERGE INTO` require Delta tables
- Use `MERGE INTO` for upserts — preferred over `INSERT` + `UPDATE`
- Deduplicate source before MERGE to avoid "multiple rows matched" error:

```sql
MERGE INTO target AS t
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
        FROM source
    ) WHERE rn = 1
) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

## Complex Types

- Use `TRANSFORM` / `FILTER` / `EXISTS` / `AGGREGATE` for array/map HOFs
- Use `LATERAL VIEW explode` for flattening arrays into rows
- Prefer `array_contains` over `explode` for membership checks (cheaper)

```sql
-- HOF pattern
SELECT TRANSFORM(tags, t -> UPPER(t)) AS tags_upper FROM events;

-- Membership check (cheap)
SELECT * FROM events WHERE array_contains(tags, 'priority');

-- Flatten (expensive — use when you need rows)
SELECT name, tag
FROM events
LATERAL VIEW explode(tags) AS tag;
```

## NULL Handling

- Never compare with `= NULL` — use `IS NULL` / `IS NOT NULL`
- Use `<=>` (null-safe equality) for join conditions that may contain NULLs
- Prefer `COALESCE(col, default)` over `IFNULL` for portability

## Comments

- File-level comment at the top of every SQL file explaining purpose
- Use `--` for single-line comments, `/* */` for multi-line
- Mark complex logic with inline comments

```sql
-- Computes rolling 7-day revenue per customer
WITH base AS (
    SELECT
        customer_id,
        order_date,
        amount,
        /* running sum over the last 7 days including today */
        SUM(amount) OVER (
            PARTITION BY customer_id
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_revenue
    FROM orders
)
SELECT * FROM base;
```

## Performance Conventions

- Push filters as early as possible (before joins and aggregations)
- Filter on partition columns when available
- Use `/*+ BROADCAST(dim) */` hint for small dimension tables
- Avoid UDFs in WHERE clauses — they block predicate pushdown
- Prefer window functions over self-joins for row comparisons

## File Organisation

| Path pattern | Content |
|-------------|---------|
| `src/<topic>/` | SQL examples by topic |
| `src/function/` | Function-specific SQL demos |
| `src/application/` | End-to-end application queries |
