-- Partition coalescing examples in Spark SQL (Databricks dialect).
-- Covers COALESCE to reduce output files, partition pruning, the REBALANCE hint,
-- and how to inspect partition counts via EXPLAIN.

CREATE OR REPLACE TEMP VIEW transactions AS
SELECT *
FROM
    VALUES
    (1, '2024-01-05', 'US', 100.0),
    (2, '2024-01-12', 'CA', 55.0),
    (3, '2024-02-03', 'US', 210.0),
    (4, '2024-02-18', 'CA', 90.0),
    (5, '2024-03-07', 'UK', 330.0),
    (6, '2024-03-22', 'US', 75.0),
    (7, '2024-04-01', 'CA', 180.0),
    (8, '2024-04-14', 'UK', 40.0)
        AS transactions (id, txn_date, region, amount);

---
-- 1. COALESCE hint — reduce to fewer output partitions (no full shuffle)
-- Use when the current stage already has many partitions and the result is small.
---

-- Reduce to a single output partition (one output file)
SELECT /*+ COALESCE(1) */
    region,
    SUM(amount) AS total
FROM transactions
GROUP BY region;
-- Result: all output rows go into 1 partition

-- Reduce to 2 partitions
SELECT /*+ COALESCE(2) */ *
FROM transactions
ORDER BY txn_date;

---
-- 2. COALESCE vs REPARTITION
--
--   COALESCE(n)       — merges existing partitions locally, NO shuffle
--                       fast and cheap; output may be uneven (some partitions larger)
--   REPARTITION(n)    — full shuffle, distributes data evenly across exactly n partitions
--                       slower (network transfer) but balanced output
--
-- Rule of thumb:
--   Use COALESCE when shrinking (e.g., after a heavy filter reduces data volume).
--   Use REPARTITION when you need guaranteed evenness or hash-partitioning.
---

-- COALESCE (no shuffle — partition boundaries merge in place)
EXPLAIN
SELECT /*+ COALESCE(2) */ * FROM transactions;

-- REPARTITION (full shuffle — data redistributed across the network)
EXPLAIN
SELECT /*+ REPARTITION(2) */ * FROM transactions;
-- In the REPARTITION plan you will see "Exchange" node; COALESCE omits it.

---
-- 3. Partition pruning — push filters to skip reading irrelevant partitions
-- In a real Delta/Parquet table partitioned by "region", Spark will skip
-- files that don't match the predicate.
---

-- Simulate partition-column filter
SELECT *
FROM transactions
WHERE region = 'US';
-- Result: Spark skips reading partitions for CA and UK entirely

-- Compound partition filter
SELECT *
FROM transactions
WHERE
    region IN ('US', 'CA')
    AND txn_date >= '2024-03-01';

-- EXPLAIN shows PartitionFilters and DataFilters separately
EXPLAIN
SELECT *
FROM transactions
WHERE
    region = 'US'
    AND txn_date >= '2024-02-01';

---
-- 4. REBALANCE hint (Spark 3.3+ with AQE enabled)
-- AQE re-partitions the output to roughly equal-sized partitions without you
-- specifying the exact count. Ideal after heavy filters that create data skew.
---

SET spark.sql.adaptive.enabled = TRUE;

SELECT /*+ REBALANCE */ *
FROM transactions
WHERE region = 'US';
-- AQE inspects runtime statistics and coalesces or splits partitions automatically

-- REBALANCE by a specific column (like REPARTITION but AQE-driven)
SELECT /*+ REBALANCE(region) */
    id,
    txn_date,
    region,
    amount
FROM transactions;

---
-- 5. Verifying partition count via EXPLAIN
---

-- Count shuffle output partitions visible in the physical plan
EXPLAIN FORMATTED
SELECT /*+ COALESCE(1) */
    region,
    COUNT(*) AS cnt,
    SUM(amount) AS total
FROM transactions
GROUP BY region;
-- Look for "CoalesceExec" node with numPartitions=1 in the plan

EXPLAIN FORMATTED
SELECT /*+ REBALANCE */ *
FROM transactions;
-- Look for "AQEShuffleRead" with "localShuffleReader" or "coalesced" annotations

---
-- 6. Practical pattern — reading with partition pruning then coalescing output
---

SET spark.sql.shuffle.partitions = 4;

-- Step 1: filter early (partition pruning kicks in on the source)
-- Step 2: aggregate (shuffle with 4 partitions per the setting above)
-- Step 3: coalesce result to 1 file for downstream consumption
SELECT /*+ COALESCE(1) */
    region,
    DATE_FORMAT(txn_date, 'yyyy-MM') AS month,
    SUM(amount) AS monthly_total,
    COUNT(*) AS txn_count
FROM transactions
WHERE region = 'US'
GROUP BY
    region,
    DATE_FORMAT(txn_date, 'yyyy-MM')
ORDER BY month;
