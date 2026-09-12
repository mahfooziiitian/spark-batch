# :material-counter: COUNT

`COUNT` measures row presence, not numeric magnitude. Spark SQL offers row counts, non-`NULL` value counts, distinct counts, and conditional counts.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[COUNT(*)] --> D[Counts all rows]
    B[COUNT(col)] --> E[Counts non-NULL values]
    C[COUNT(DISTINCT ...)] --> F[Deduplicate then count]
```

### :material-animation-play: Interactive Visualization — COUNT Null Rules

<div id="viz-count-null-rules" class="ts-viz"></div>

Click through the variants to see which rows remain visible to each form of `COUNT`.

______________________________________________________________________

## :material-pin: Syntax

```sql
COUNT(*)
COUNT(expr)
COUNT(DISTINCT expr)
COUNT(DISTINCT expr1, expr2)
COUNT(*) FILTER (WHERE condition)
```

______________________________________________________________________

## :material-magnify: Verified behavior

1. `COUNT(*)` and `COUNT(1)` both counted every row in our PySpark 4.2 test.
2. `COUNT(expr)` skips only rows where that expression is `NULL`.
3. `COUNT(DISTINCT expr)` excludes `NULL` before deduplicating.
4. `COUNT(DISTINCT a, b)` counted only distinct tuples where **both** expressions were non-`NULL`.
5. `COUNT(*)` is the only aggregate in this family that returns `0` rather than `NULL` on an empty input.

______________________________________________________________________

## :material-flask-outline: Practical examples

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
    (1,  'East',  'Widget', 120.00),
    (2,  'West',  'Gadget', 340.00),
    (3,  'East',  'Widget',  80.00),
    (4,  'North', 'Gadget', 210.00),
    (5,  'West',  'Widget', 150.00),
    (6,  'East',  'Gadget', 450.00),
    (7,  'North', 'Widget',  90.00),
    (8,  'West',  'Gadget', 270.00),
    (9,  'East',  NULL,     NULL),
    (10, NULL,    'Widget',  50.00)
AS sales(order_id, region, product, amount);
```

### Comparing variants

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(region) AS rows_with_region,
    COUNT(product) AS rows_with_product,
    COUNT(amount) AS rows_with_amount,
    COUNT(DISTINCT region) AS distinct_regions,
    COUNT(DISTINCT region, product) AS distinct_region_product_pairs,
    COUNT(1) AS count_one
FROM sales;
```

Verified PySpark 4.2 result:

| total_rows | rows_with_region | rows_with_product | rows_with_amount | distinct_regions | distinct_region_product_pairs | count_one |
| ---------- | ---------------- | ----------------- | ---------------- | ---------------- | ----------------------------- | --------- |
| 10         | 9                | 9                 | 9                | 3                | 6                             | 10        |

### Conditional counts

```sql
SELECT
    COUNT(*) FILTER (WHERE region = 'East') AS east_orders,
    COUNT(*) FILTER (WHERE amount > 200) AS high_value_orders,
    COUNT(*) FILTER (WHERE product IS NULL) AS missing_product_orders
FROM sales;
```

### Empty input

```sql
SELECT
    COUNT(*) AS zero_not_null,
    SUM(amount) AS sum_is_null
FROM sales
WHERE region = 'South';
```

Verified result: `COUNT(*) = 0`, `SUM(amount) = NULL`.

______________________________________________________________________

## :material-brain: When to use

| Scenario                             | Recommended pattern           |
| ------------------------------------ | ----------------------------- |
| Count every row                      | `COUNT(*)`                    |
| Count populated values               | `COUNT(col)`                  |
| Count unique values                  | `COUNT(DISTINCT col)`         |
| Count unique non-null tuples         | `COUNT(DISTINCT col1, col2)`  |
| Count only rows matching a predicate | `COUNT(*) FILTER (WHERE ...)` |
