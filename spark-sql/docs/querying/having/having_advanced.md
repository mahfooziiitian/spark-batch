# :material-lightning-bolt: Advanced HAVING

Advanced `HAVING` usage in Spark SQL includes multi-level grouping, alias resolution, `QUALIFY`, and single-group aggregations without an explicit `GROUP BY`.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — Grouping Level Explorer

<div id="viz-having-advanced" class="ts-viz"></div>

Switch between ordinary grouping, subtotal-producing grouping, and single-group aggregation to see how many grouped rows exist before `HAVING` starts filtering them.

<script src="../../assets/js/querying-having-viz.js"></script>

______________________________________________________________________

## :material-layers: HAVING with ROLLUP

`ROLLUP` generates detail rows, subtotals, and a grand total. `HAVING` can remove low-value groups after those grouping levels have been produced.

```sql
SELECT
    COALESCE(region, 'ALL') AS region,
    COALESCE(product, 'ALL') AS product,
    SUM(amount) AS total_revenue
FROM orders
GROUP BY ROLLUP(region, product)
HAVING SUM(amount) > 100000
ORDER BY region NULLS LAST, product NULLS LAST;
```

______________________________________________________________________

## :material-grid: HAVING with CUBE

```sql
SELECT
    COALESCE(CAST(YEAR(order_date) AS STRING), 'ALL YEARS') AS year,
    COALESCE(region, 'ALL REGIONS') AS region,
    SUM(amount) AS revenue
FROM orders
GROUP BY CUBE(YEAR(order_date), region)
HAVING SUM(amount) > 50000;
```

`CUBE` creates every grouping combination, then `HAVING` trims the combinations you do not want to keep.

______________________________________________________________________

## :material-format-list-group: HAVING with GROUPING SETS

```sql
SELECT
    region,
    product_category,
    SUM(amount) AS total_revenue,
    COUNT(*) AS order_count
FROM orders
GROUP BY GROUPING SETS (
    (region),
    (product_category),
    ()
)
HAVING SUM(amount) > 25000;
```

### Using `GROUPING()` to identify subtotal rows

```sql
SELECT
    CASE WHEN GROUPING(region) = 1 THEN 'ALL REGIONS' ELSE region END AS region_label,
    CASE WHEN GROUPING(product) = 1 THEN 'ALL PRODUCTS' ELSE product END AS product_label,
    SUM(amount) AS revenue
FROM orders
GROUP BY ROLLUP(region, product)
HAVING SUM(amount) > 10000;
```

______________________________________________________________________

## :material-tag-text-outline: Aliases and Grouped Expressions

Spark 4.2 resolves both aggregate aliases and grouped-expression aliases in `HAVING`.

```sql
SELECT
    upper(region) AS region_key,
    SUM(amount) AS total_revenue
FROM orders
GROUP BY region_key
HAVING region_key = 'APAC'
   AND total_revenue > 100000;
```

This was verified in PySpark 4.2 with both the grouping alias and the aggregate alias referenced directly in `HAVING`.

______________________________________________________________________

## :material-window-maximize: QUALIFY and HAVING

`HAVING` filters aggregate results. `QUALIFY` filters rows after window functions have been computed.

```sql
SELECT
    customer_id,
    order_id,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_id DESC
    ) AS rn
FROM orders
QUALIFY rn = 1;
```

```sql
SELECT
    region,
    product_id,
    SUM(amount) AS revenue,
    RANK() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) AS revenue_rank
FROM orders
GROUP BY region, product_id
HAVING SUM(amount) > 0
QUALIFY revenue_rank <= 3;
```

!!! note

    `QUALIFY` is not part of ANSI SQL, but PySpark 4.2 does support it. The fallback on engines without `QUALIFY` is still a subquery plus outer `WHERE`.

______________________________________________________________________

## :material-table-of-contents: HAVING without GROUP BY

Without an explicit `GROUP BY`, Spark uses a single implicit group only when the projection is aggregate-safe.

```sql
SELECT
    COUNT(*) AS row_count
FROM orders
HAVING COUNT(*) >= 1000;
```

```sql
SELECT
    AVG(amount) AS global_avg
FROM orders
HAVING COUNT(*) > 0;
```

Raw columns do not become magically available:

```sql
SELECT amount
FROM orders
HAVING COUNT(*) > 0;
```

PySpark 4.2 rejects that last query with `MISSING_GROUP_BY`.

______________________________________________________________________

## :material-magnify: Behavior Notes

1. `ROLLUP(a, b)` yields `(a, b)`, `(a)`, and `()`.
2. `CUBE(a, b)` yields `(a, b)`, `(a)`, `(b)`, and `()`.
3. `GROUPING SETS` keeps only the combinations you name explicitly.
4. `HAVING` can filter subtotal rows and grand-total rows just like ordinary grouped rows.
5. In Spark 4.2, alias resolution in `HAVING` works for grouped expressions and aggregate aliases.
