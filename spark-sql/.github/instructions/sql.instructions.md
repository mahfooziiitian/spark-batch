---
applyTo: "src/**/*.sql"
---

# SQL Instructions — Spark SQL (Databricks)

## Dialect & Linter

- **Dialect**: Databricks (`spark.sql.ansi.enabled = true` implied)
- **Linter / Formatter**: SQLFluff — config lives in `[tool.sqlfluff.*]` sections of `pyproject.toml`
- Run lint:   `uv run task sql_lint`
- Auto-fix:   `uv run task sql_format`
- Combined:   `uv run task sql`

## SQLFluff Rules (from `pyproject.toml`)

| Rule | Setting |
|------|---------|
| Dialect | `databricks` |
| Keyword capitalisation | `UPPER` |
| Identifier capitalisation | `lower` |
| Indented joins | `false` |
| Indented USING/ON | `true` |
| Max line length | 128 |
| File extensions | `.sql`, `.sql.j2`, `.dml`, `.ddl` |

## Formatting Style

```sql
-- ✅ CORRECT — keywords UPPER, columns indented, one clause per line
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
select order_id,sum(amount) from orders where order_date>='2024-01-01' group by order_id;
```

## File Header Comments

Every SQL file must start with a file-level comment:

```sql
-- ============================================================
-- Topic: SCD Type 2 — versioned customer dimension
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Tracks full history using start_date/end_date
--              and is_current flag. Two-step MERGE pattern.
-- ============================================================
```

## CTEs

- Use CTEs (`WITH`) over subqueries for readability.
- One CTE per logical transformation step.
- CTE names use `snake_case`.

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

- `UPDATE`, `DELETE`, `MERGE INTO` require Delta tables (`USING DELTA`).
- Use `MERGE INTO` for upserts — preferred over separate `INSERT` + `UPDATE`.
- **Deduplicate the source before MERGE** — Delta raises an error if multiple source rows match one target row:

```sql
MERGE INTO target AS t
USING (
    SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
        FROM source
    )
    WHERE rn = 1
) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

## SCD Patterns

### Row Hash for Change Detection

Always use `md5(concat_ws('||', col1, col2, ...))` — include every tracked column in the hash:

```sql
md5(concat_ws('||', name, email, city)) AS row_hash
```

Use `<=>` (null-safe equality) if any tracked column can be NULL:

```sql
WHERE NOT (src.row_hash <=> tgt.row_hash)
```

### SCD Type 1 — Single MERGE

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT *, md5(concat_ws('||', name, email, city)) AS row_hash
    FROM staging_customer
) AS src
ON src.customer_id = tgt.customer_id
WHEN MATCHED AND src.row_hash <> tgt.row_hash THEN
    UPDATE SET name = src.name, email = src.email, city = src.city,
               row_hash = src.row_hash, updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash, updated_at)
    VALUES (src.customer_id, src.name, src.email, src.city, src.row_hash, current_timestamp());
```

### SCD Type 2 — Two-Step MERGE

A single MERGE cannot expire an existing row **and** insert a new sibling row for the same key.
Always use two separate statements:

```sql
-- Step 1: expire changed rows
MERGE INTO dim_customer AS tgt
USING (SELECT customer_id, md5(concat_ws('||', name, email, city)) AS row_hash FROM staging_customer) AS src
ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE AND tgt.row_hash <> src.row_hash
WHEN MATCHED THEN UPDATE SET end_date = current_timestamp(), is_current = FALSE;

-- Step 2: insert new versions
MERGE INTO dim_customer AS tgt
USING (
    SELECT s.customer_id, s.name, s.email, s.city,
           md5(concat_ws('||', s.name, s.email, s.city)) AS row_hash
    FROM staging_customer AS s
    LEFT JOIN dim_customer AS d ON d.customer_id = s.customer_id AND d.is_current = TRUE
    WHERE d.customer_id IS NULL OR d.row_hash <> md5(concat_ws('||', s.name, s.email, s.city))
) AS src
ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE AND tgt.row_hash = src.row_hash
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash, start_date, end_date, is_current)
    VALUES (src.customer_id, src.name, src.email, src.city, src.row_hash,
            current_timestamp(), NULL, TRUE);
```

## Window Functions

```sql
-- Running total partitioned by region
SUM(amount) OVER (
    PARTITION BY region
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_total,

-- Rank within partition
RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS revenue_rank,

-- LAG / LEAD for period-over-period comparison
LAG(amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount
```

## Complex Types

- Use `TRANSFORM` / `FILTER` / `AGGREGATE` HOFs for arrays and maps.
- Use `LATERAL VIEW explode` to flatten arrays into rows.
- Prefer `array_contains` over `explode` for membership checks.

```sql
-- HOF: upper-case each tag
SELECT TRANSFORM(tags, t -> UPPER(t)) AS tags_upper FROM events;

-- Membership check (predicate pushdown friendly)
SELECT * FROM events WHERE array_contains(tags, 'priority');

-- Flatten to rows (use when you need one row per element)
SELECT name, tag FROM events LATERAL VIEW explode(tags) AS tag;
```

## NULL Handling

- Never use `= NULL` — always `IS NULL` / `IS NOT NULL`.
- Use `<=>` (null-safe equality) in join conditions that may contain NULLs.
- Prefer `COALESCE(col, default)` over `IFNULL` for portability.

## Performance Conventions

- Push `WHERE` filters before joins and aggregations.
- Filter on partition columns when available.
- Use `/*+ BROADCAST(dim) */` hint for small dimension tables (< ~10 MB).
- Avoid UDFs in `WHERE` clauses — they disable predicate pushdown.
- Prefer window functions over self-joins for row comparisons.
- Use `OPTIMIZE ... ZORDER BY (col)` after bulk writes to Delta tables.

```sql
OPTIMIZE dim_customer ZORDER BY (customer_id);
VACUUM dim_customer RETAIN 168 HOURS;
```

## File Organisation

| Path pattern | Content |
|-------------|---------|
| `src/scd/type1/` | SCD Type 1 SQL examples |
| `src/scd/type2/` | SCD Type 2 SQL examples |
| `src/timeseries/` | Tumbling, hopping, sliding, session, gap-fill |
| `src/function/` | Function-specific SQL demos |
| `src/application/` | End-to-end application queries |
| `src/<topic>/` | SQL examples organised by topic |
