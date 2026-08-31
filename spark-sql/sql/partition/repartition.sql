-- Repartitioning examples in Spark SQL (Databricks dialect).
-- Demonstrates SQL hints and settings that control the number and layout of
-- output partitions, which directly affects shuffle performance and file counts.

CREATE OR REPLACE TEMP VIEW sales AS
SELECT *
FROM
    VALUES
    (1, 'Alice', 'US', 250.0),
    (2, 'Bob', 'CA', 120.0),
    (3, 'Carol', 'US', 310.0),
    (4, 'Dana', 'CA', 75.0),
    (5, 'Eve', 'UK', 430.0),
    (6, 'Frank', 'US', 190.0)
        AS sales (id, name, region, amount);

---
-- 1. REPARTITION hint — fixed number of partitions
-- Inserts a full shuffle to redistribute data evenly across N partitions.
---

SELECT /*+ REPARTITION(4) */
    id,
    name,
    region,
    amount
FROM sales;
-- Result: output will have exactly 4 shuffle partitions

---
-- 2. REPARTITION hint — partition by column
-- Guarantees all rows with the same key land in the same partition (hash partitioning).
---

SELECT /*+ REPARTITION(4, region) */
    id,
    name,
    region,
    amount
FROM sales;
-- Result: 4 partitions, each containing a consistent subset of regions

-- Repartition by multiple columns
SELECT /*+ REPARTITION(8, region, name) */ *
FROM sales;

---
-- 3. COALESCE hint — reduce partitions without full shuffle
-- Merges adjacent partitions locally (no network transfer of existing data).
---

SELECT /*+ COALESCE(1) */
    region,
    SUM(amount) AS total
FROM sales
GROUP BY region;
-- Result: output collapsed to 1 partition (good for small result sets)

---
-- 4. SET shuffle partitions at session level
-- Controls how many partitions the shuffle stage produces for aggregations/joins.
---

SET spark.sql.shuffle.partitions = 8;

SELECT
    region,
    SUM(amount) AS total
FROM sales
GROUP BY region;
-- Result: shuffle will produce up to 8 output partitions

-- Reset to Spark default (200) or a sensible AQE-managed value
SET spark.sql.shuffle.partitions = 200;

-- Enable Adaptive Query Execution to let Spark auto-tune partition count
SET spark.sql.adaptive.enabled = TRUE;

---
-- 5. EXPLAIN to verify partitioning in the physical plan
---

EXPLAIN
SELECT /*+ REPARTITION(4, region) */
    region,
    SUM(amount) AS total
FROM sales
GROUP BY region;
-- Look for "RoundRobinPartitioning" or "HashPartitioning(region, 4)" in the plan

EXPLAIN FORMATTED
SELECT /*+ COALESCE(2) */ *
FROM sales;

---
-- 6. Use case: repartition before writing to avoid small files
-- Without repartitioning a large table write may produce hundreds of tiny files.
---

SET spark.sql.shuffle.partitions = 4;

-- Write-time repartition: 4 output files per region (partition column)
SELECT /*+ REPARTITION(4, region) */
    id,
    name,
    region,
    amount
FROM sales;

-- In practice you would follow this SELECT with:
-- INSERT INTO target_table PARTITION (region) ...
-- or use the DataFrame writer:
--   df.repartition(4, col("region")).write.partitionBy("region").parquet(path)

---
-- 7. Comparison summary (comment)
--
--   REPARTITION(n)        — full shuffle, exactly n partitions, even distribution
--   REPARTITION(n, col)   — full shuffle, hash-partitioned by col, n partitions
--   COALESCE(n)           — no shuffle, reduces partitions by merging, may be skewed
--   AQE (adaptive.enabled)— auto-coalesces post-shuffle partitions based on stats
---
