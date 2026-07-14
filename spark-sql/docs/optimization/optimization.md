# :material-tune: Optimization Techniques

A practical checklist of Spark SQL optimisations ordered by impact and ease of application.

---

## :material-magnify: I/O Optimisations

### Column Pruning — select only what you need

```sql
-- Bad: reads all columns from Parquet
SELECT * FROM orders WHERE region = 'US';

-- Good: only 3 columns read from storage
SELECT order_id, amount, region FROM orders WHERE region = 'US';
```

### Predicate Pushdown — filter early

```sql
-- Predicate pushed to Parquet row-group scan — reads far less data
SELECT order_id, amount
FROM orders
WHERE region = 'US' AND amount > 500 AND order_date >= '2024-01-01';

-- UDFs BLOCK pushdown — avoid in WHERE when predicates can be expressed in SQL
-- Bad:
SELECT * FROM orders WHERE my_udf(region) = 'US';
-- Good:
SELECT * FROM orders WHERE region = 'US';
```

### Partition Pruning

```sql
-- Partitioned table: filter on partition column skips entire directories
SELECT * FROM events
WHERE event_date = '2024-06-01'   -- partition prune
  AND event_type = 'click';        -- row-group filter

-- Avoid functions on partition columns — they break pruning
-- Bad:  WHERE YEAR(event_date) = 2024
-- Good: WHERE event_date >= '2024-01-01' AND event_date < '2025-01-01'
```

### Use Columnar Formats

```sql
-- Create table in Parquet or Delta — columnar, compressed, splittable
CREATE TABLE analytics.orders
USING DELTA
PARTITIONED BY (order_date);

-- Avoid CSV for large production tables — no column pruning, no stats
```

---

## :material-lan-connect: Join Optimisations

### Broadcast Small Dimension Tables

```sql
-- Hint: force broadcast even if table is above autoBroadcastJoinThreshold
SELECT /*+ BROADCAST(d) */ f.order_id, d.region_name
FROM fact_orders f
JOIN dim_region d ON f.region_id = d.id;

-- Config: raise global threshold (100 MB)
SET spark.sql.autoBroadcastJoinThreshold = 104857600;
```

### Collect Statistics for Cost-Based Optimiser (CBO)

```sql
-- Without stats, Catalyst uses defaults — may choose wrong join order
ANALYZE TABLE orders COMPUTE STATISTICS;
ANALYZE TABLE customers COMPUTE STATISTICS FOR COLUMNS id, region, status;

-- Enable CBO (default in Spark 3.x)
SET spark.sql.cbo.enabled = true;
SET spark.sql.cbo.joinReorder.enabled = true;
```

### Avoid Cartesian / Cross Joins

```sql
-- Dangerous — every row × every row
SELECT * FROM a, b;           -- implicit cross join
SELECT * FROM a CROSS JOIN b; -- explicit cross join

-- Always specify a join condition
SELECT a.*, b.name FROM a JOIN b ON a.id = b.id;
```

### Pre-filter Before Joining

```sql
-- Filter each side before the join to minimise shuffle data
SELECT f.order_id, d.quarter
FROM (SELECT * FROM fact_orders WHERE order_date >= '2024-01-01') f
JOIN (SELECT * FROM dim_date WHERE year = 2024) d
  ON f.order_date = d.date_key;
```

---

## :material-sigma: Aggregation Optimisations

### Partial Aggregation (Map-Side Combine)

Spark automatically applies partial aggregation before the shuffle for `SUM`,
`COUNT`, `MIN`, `MAX`. Ensure you do not suppress this with `DISTINCT` inside aggregates
unless needed.

```sql
-- Good: partial agg reduces shuffle data
SELECT region, SUM(amount) FROM orders GROUP BY region;

-- Expensive: COUNT DISTINCT forces a full shuffle with no partial reduction
SELECT region, COUNT(DISTINCT customer_id) FROM orders GROUP BY region;

-- Alternative: approximate distinct count (faster, slight inaccuracy)
SELECT region, APPROX_COUNT_DISTINCT(customer_id) FROM orders GROUP BY region;
```

### Filter Before GROUP BY

```sql
-- Push filters to WHERE, not HAVING, to reduce rows in the aggregation
-- Bad (HAVING filters after aggregation):
SELECT region, SUM(amount) AS total
FROM orders
GROUP BY region
HAVING region = 'US';

-- Good (WHERE filters before aggregation):
SELECT region, SUM(amount) AS total
FROM orders
WHERE region = 'US'
GROUP BY region;
```

---

## :material-scale-unbalanced: Skew Handling

```sql
-- AQE handles most skew automatically — ensure it is enabled
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;

-- For extreme skew (single key with billions of rows), use key salting
-- Step 1: Add random salt to hot key
WITH salted AS (
    SELECT
        CONCAT(CAST(customer_id AS STRING), '_',
               CAST(FLOOR(RAND() * 10) AS STRING)) AS salted_key,
        amount
    FROM orders
    WHERE customer_id = 12345   -- the hot key
    UNION ALL
    SELECT CAST(customer_id AS STRING), amount
    FROM orders
    WHERE customer_id != 12345
)
SELECT SPLIT(salted_key, '_')[0] AS customer_id, SUM(amount)
FROM salted
GROUP BY SPLIT(salted_key, '_')[0];
```

---

## :material-database-arrow-down: Storage Optimisations

```sql
-- [Databricks] Compact small files in a partition
OPTIMIZE sales WHERE order_date >= '2024-01-01';

-- [Databricks] Z-ORDER for multi-column skipping within a partition
OPTIMIZE sales ZORDER BY (region, product_id);

-- [Databricks] Remove old file versions
VACUUM sales RETAIN 168 HOURS;  -- keep 7 days

-- Bucketing: co-locate join keys — eliminates shuffle for repeated joins
CREATE TABLE orders_bucketed
USING PARQUET
CLUSTERED BY (customer_id) INTO 200 BUCKETS
AS SELECT * FROM orders;
```

---

## :material-cached: Reuse Optimisations

```sql
-- Cache a heavy subquery used in multiple downstream queries
CACHE TABLE clean_orders AS
SELECT order_id, LOWER(TRIM(region)) AS region, amount, order_date
FROM raw_orders
WHERE order_id IS NOT NULL;

-- Use CTEs to prevent repeated evaluation
WITH monthly_totals AS (
    SELECT DATE_TRUNC('month', order_date) AS month, SUM(amount) AS total
    FROM orders
    GROUP BY 1
)
SELECT month, total, total / SUM(total) OVER () AS share
FROM monthly_totals;
```

---

## :material-brain: Optimisation Checklist

| # | Check | Action |
|---|-------|--------|
| 1 | Selecting only needed columns? | Remove `SELECT *` |
| 2 | Filters on partition columns? | Use partition column in `WHERE` |
| 3 | No functions on partition columns in `WHERE`? | Replace `YEAR(col)` with range |
| 4 | Small dimension tables broadcast? | Use hint or raise threshold |
| 5 | Table statistics collected? | `ANALYZE TABLE … COMPUTE STATISTICS` |
| 6 | No UDFs blocking pushdown? | Rewrite as SQL |
| 7 | AQE enabled? | `SET spark.sql.adaptive.enabled = true` |
| 8 | Output file sizes reasonable (64–256 MB)? | Use `REBALANCE` or `OPTIMIZE` |
| 9 | Repeated subquery moved to CTE or cache? | Use CTE or `CACHE TABLE` |
| 10 | Skew visible in Spark UI? | Lower AQE skew thresholds |
