# :material-layers-triple: GROUPING SETS

`GROUPING SETS` lets you compute aggregates for multiple, explicitly listed grouping combinations in a single query — without writing multiple `UNION ALL` branches.

---

## :material-pin: Syntax

```sql
SELECT col1 [, col2, ...], agg_func(expr) [AS alias]
FROM   table_name
GROUP BY GROUPING SETS (
    (col1, col2),   -- grouping combination 1
    (col1),         -- grouping combination 2
    (col2),         -- grouping combination 3
    ()              -- grand total (empty grouping)
);
```

| Element | Description |
|---------|-------------|
| `(col1, col2)` | Aggregate by both columns together |
| `(col1)` | Aggregate by `col1` only |
| `()` | Empty set — produces one grand-total row |

### Equivalences

```sql
-- ROLLUP(a, b) is equivalent to:
GROUPING SETS ((a, b), (a), ())

-- CUBE(a, b) is equivalent to:
GROUPING SETS ((a, b), (a), (b), ())
```

---

## :material-magnify: Behavior

1. **NULL as grouping marker** — columns not included in a specific grouping set appear as `NULL` in the output rows for that set; these are synthetic NULLs, not data NULLs.
2. **`GROUPING(col)`** — returns `1` for a synthetic NULL (grouping placeholder) and `0` for a real grouping value; use it to label or filter subtotal rows.
3. **UNION ALL equivalence** — `GROUPING SETS` is logically equivalent to a `UNION ALL` of separate `GROUP BY` queries, but executes in a single scan which is far more efficient.
4. **Duplicate sets** — listing the same set twice produces duplicate output rows; avoid duplicates.
5. **Any combination** — unlike `ROLLUP` and `CUBE`, you can mix arbitrary column subsets in any order, including non-hierarchical combinations.

---

## :material-flask-outline: Practical Examples

### Setup

```sql
CREATE TABLE sales (
    order_id   BIGINT,
    region     STRING,
    product    STRING,
    amount     DOUBLE,
    order_date DATE
);

INSERT INTO sales VALUES
    (1, 'East',  'Widget',  120.00, DATE '2024-01-15'),
    (2, 'West',  'Gadget',  340.00, DATE '2024-01-15'),
    (3, 'East',  'Widget',   80.00, DATE '2024-02-10'),
    (4, 'North', 'Gadget',  210.00, DATE '2024-02-10'),
    (5, 'West',  'Widget',  150.00, DATE '2024-03-05'),
    (6, 'East',  'Gadget',  450.00, DATE '2024-03-05'),
    (7, 'North', 'Widget',   90.00, DATE '2024-03-20'),
    (8, 'West',  'Gadget',  270.00, DATE '2024-03-20');
```

### 1 — Custom subtotals: `(region, product)`, `(region)`, grand total

```sql
SELECT
    region,
    product,
    SUM(amount) AS total_sales
FROM sales
GROUP BY GROUPING SETS (
    (region, product),
    (region),
    ()
)
ORDER BY region NULLS LAST, product NULLS LAST;
-- Result:
-- region | product | total_sales
-- --------|---------|------------
-- East    | Gadget  | 450.0       ← (region, product)
-- East    | Widget  | 200.0       ← (region, product)
-- East    | NULL    | 650.0       ← (region)
-- North   | Gadget  | 210.0
-- North   | Widget  | 90.0
-- North   | NULL    | 300.0       ← (region)
-- West    | Gadget  | 610.0
-- West    | Widget  | 150.0
-- West    | NULL    | 760.0       ← (region)
-- NULL    | NULL    | 1710.0      ← grand total ()
```

### 2 — Equivalence to UNION ALL

The following two queries produce identical results:

```sql
-- GROUPING SETS (single scan — preferred)
SELECT region, NULL AS product, SUM(amount) AS total_sales
FROM sales
GROUP BY GROUPING SETS ((region), ());

-- Equivalent UNION ALL (two separate scans)
SELECT region, NULL AS product, SUM(amount) AS total_sales
FROM sales
GROUP BY region
UNION ALL
SELECT NULL AS region, NULL AS product, SUM(amount) AS total_sales
FROM sales;
```

### 3 — Non-hierarchical combinations: `(region)` and `(product)` without the cross

```sql
SELECT
    region,
    product,
    SUM(amount)       AS total_sales,
    GROUPING(region)  AS g_region,
    GROUPING(product) AS g_product
FROM sales
GROUP BY GROUPING SETS (
    (region),
    (product)
)
ORDER BY g_region, g_product, region NULLS LAST, product NULLS LAST;
-- Result:
-- region | product | total_sales | g_region | g_product
-- --------|---------|-------------|----------|----------
-- East    | NULL    | 650.0       | 0        | 1
-- North   | NULL    | 300.0       | 0        | 1
-- West    | NULL    | 760.0       | 0        | 1
-- NULL    | Gadget  | 1270.0      | 1        | 0
-- NULL    | Widget  | 440.0       | 1        | 0
```

### 4 — Readable labels with `GROUPING()`

```sql
SELECT
    CASE WHEN GROUPING(region)  = 1 THEN 'All Regions'  ELSE region  END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'All Products' ELSE product END AS product_label,
    SUM(amount) AS total_sales
FROM sales
GROUP BY GROUPING SETS (
    (region, product),
    (region),
    (product),
    ()
)
ORDER BY region_label, product_label;
```

### 5 — Three-column GROUPING SETS (select specific combinations)

```sql
SELECT
    YEAR(order_date) AS yr,
    region,
    product,
    SUM(amount)      AS total_sales
FROM sales
GROUP BY GROUPING SETS (
    (YEAR(order_date), region, product),  -- full detail
    (YEAR(order_date), region),           -- year + region subtotal
    (region),                             -- region across all years
    ()                                    -- grand total
)
ORDER BY yr NULLS LAST, region NULLS LAST, product NULLS LAST;
```

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Need exactly N custom subtotal combinations | `GROUPING SETS(...)` |
| Replace multiple `UNION ALL GROUP BY` queries | `GROUPING SETS` (single scan) |
| Mix hierarchical and non-hierarchical groupings | `GROUPING SETS` |
| All 2ⁿ combinations of n columns | [`CUBE`](cube.md) |
| Strict left-to-right hierarchy | [`ROLLUP`](rollup.md) |
| Distinguish data NULLs from grouping NULLs | `GROUPING(col)` |
