# :material-content-duplicate: CTE for Deduplication

CTEs plus window functions are a standard Spark SQL dedup pattern. In Spark 4.2 you can still use the classic `WITH ranked AS (...) SELECT ... WHERE rn = 1`, but you can also express the final filter with `QUALIFY` when you do not need to reuse the ranked rows.

### :material-animation-play: Interactive Visualization — Ranking and Filtering

<div id="viz-cte-dedup-steps" class="ts-viz"></div>

The animation shows duplicate groups receiving `ROW_NUMBER()` values and then being filtered down to one survivor per key. Toggle between `WHERE rn = 1` and `QUALIFY` to compare two verified Spark 4.2 shapes for the same logic.

______________________________________________________________________

## :material-information-outline: Behavior

1. **`ROW_NUMBER()` is the go-to dedup function** — it guarantees one row per partition after filtering on `1`.
2. **`QUALIFY` works in Spark 4.2** — `QUALIFY ROW_NUMBER() OVER (...) = 1` executed successfully in PySpark 4.2.
3. **CTEs remain useful even with `QUALIFY`** — use a CTE when you want to inspect duplicates, compute extra diagnostics, or feed the ranked result into another step.
4. **Tie-breakers should be deterministic when you care which row survives** — for example, `ORDER BY updated_at DESC, source_file DESC`.
5. **If you omit a meaningful tie-breaker, the surviving row is arbitrary** — `ORDER BY (SELECT NULL)` is accepted, but it intentionally does not make the result deterministic.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Keep the most recent row per key

```sql
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM staging_orders
)
SELECT order_id, customer_id, amount, status, updated_at
FROM ranked
WHERE rn = 1;
```

### Equivalent `QUALIFY` form

```sql
SELECT order_id, customer_id, amount, status, updated_at
FROM staging_orders
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY updated_at DESC
) = 1;
```

### Deduplicate before `INSERT`

```sql
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM raw_products
)
INSERT INTO dim_product
SELECT product_id, name, category, price, ingested_at
FROM deduped
WHERE rn = 1;
```

### Deduplicate before `MERGE`

```sql
WITH deduped_source AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY updated_at DESC
            ) AS rn
        FROM staging_customers
    )
    WHERE rn = 1
)
MERGE INTO dim_customer AS t
USING deduped_source AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
    name = s.name,
    email = s.email,
    city = s.city,
    updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    name,
    email,
    city,
    updated_at
) VALUES (
    s.customer_id,
    s.name,
    s.email,
    s.city,
    s.updated_at
);
```

### Flag duplicates before removing them

```sql
WITH flagged AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn,
        COUNT(*) OVER (PARTITION BY order_id) AS dup_count
    FROM staging_orders
)
SELECT *
FROM flagged
WHERE dup_count > 1
ORDER BY order_id, rn;
```

### Remove exact duplicates with an arbitrary survivor

```sql
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, customer_id, amount, status
            ORDER BY (SELECT NULL)
        ) AS rn
    FROM orders
)
SELECT order_id, customer_id, amount, status
FROM deduped
WHERE rn = 1;
```

### Keep the top 3 most recent rows per customer

```sql
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) AS rn
    FROM orders
)
SELECT customer_id, order_id, order_date, amount
FROM ranked
WHERE rn <= 3
ORDER BY customer_id, rn;
```

______________________________________________________________________

## :material-swap-horizontal: `ROW_NUMBER` vs `RANK` vs `DENSE_RANK`

| Function     | Tie behavior                 | Best use              |
| ------------ | ---------------------------- | --------------------- |
| `ROW_NUMBER` | Always unique                | True deduplication    |
| `RANK`       | Ties share rank, gaps appear | Keep all tied leaders |
| `DENSE_RANK` | Ties share rank, no gaps     | Tiering or reporting  |

For strict deduplication, `ROW_NUMBER` is the safest default because it guarantees one chosen row per partition.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                                          | Pattern                                   |
| ------------------------------------------------- | ----------------------------------------- |
| Keep the latest row per business key              | `ROW_NUMBER()` + descending timestamp     |
| Keep exactly one arbitrary copy of identical rows | `ROW_NUMBER()` + `ORDER BY (SELECT NULL)` |
| Express the filter inline                         | `QUALIFY ROW_NUMBER() ... = 1`            |
| Debug duplicates before removing them             | CTE with `dup_count` and `rn`             |
| Keep top-N records per group                      | `ROW_NUMBER()` + `WHERE rn <= N`          |
