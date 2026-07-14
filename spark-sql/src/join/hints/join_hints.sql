-- Spark join hints: BROADCAST, MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL.
-- Hints are placed in a /*+ ... */ comment immediately after the SELECT keyword
-- and override the Spark cost-based optimizer's choice of join strategy.
--
-- How hints work vs the optimizer default:
--   Without hints, Spark picks a join strategy based on table statistics:
--     • If the smaller side is under spark.sql.autoBroadcastJoinThreshold
--       (default 10 MB), it chooses a BroadcastHashJoin automatically.
--     • Otherwise it falls back to SortMergeJoin (equi-joins) or
--       BroadcastNestedLoopJoin (non-equi / cross joins).
--   With hints you tell Spark which strategy to use regardless of statistics.
--   Hints are soft: Spark silently ignores a hint it cannot satisfy
--   (e.g., BROADCAST on a table that is too large to fit in memory).
--
-- Hint reference:
--   BROADCAST(alias)           → BroadcastHashJoin — replicate the small side
--   MERGE(alias)               → SortMergeJoin     — sort both sides, merge
--   SHUFFLE_HASH(alias)        → ShuffledHashJoin  — hash-partition both sides
--   SHUFFLE_REPLICATE_NL(alias)→ BroadcastNLJoin   — nested-loop,
--                                                    no equi key needed
-- -----------------------------------------------------------------------

-- -----------------------------------------------------------------------
-- Test data
-- -----------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW orders AS
SELECT
    order_id,
    customer_id,
    amount,
    status
FROM
    VALUES
    (1, 101, 250.00, 'completed'),
    (2, 102, 80.00, 'completed'),
    (3, 101, 430.00, 'pending'),
    (4, 103, 120.00, 'completed'),
    (5, 104, 960.00, 'cancelled')
        AS orders (order_id, customer_id, amount, status);

CREATE OR REPLACE TEMP VIEW customers AS
SELECT
    customer_id,
    name,
    country
FROM
    VALUES
    (101, 'Alice', 'US'),
    (102, 'Bob', 'CA'),
    (103, 'Charlie', 'US'),
    (104, 'Diana', 'UK')
        AS customers (customer_id, name, country);

CREATE OR REPLACE TEMP VIEW products AS
SELECT
    product_id,
    name,
    category
FROM
    VALUES
    (10, 'Widget A', 'Electronics'),
    (11, 'Widget B', 'Electronics'),
    (12, 'Gadget X', 'Accessories'),
    (13, 'Gadget Y', 'Accessories')
        AS products (product_id, name, category);

CREATE OR REPLACE TEMP VIEW order_items AS
SELECT
    order_id,
    product_id,
    qty
FROM
    VALUES
    (1, 10, 2),
    (2, 11, 3),
    (3, 10, 1),
    (4, 13, 5),
    (5, 11, 2)
        AS order_items (order_id, product_id, qty);

-- -----------------------------------------------------------------------
-- 1. BROADCAST hint — replicate a small dimension table to every executor
-- -----------------------------------------------------------------------

-- Best for: small lookup / dimension tables
-- (ideally < autoBroadcastJoinThreshold).
-- Eliminates the shuffle of the large table; each executor probes a local copy.
SELECT /*+ BROADCAST(c) */ -- noqa: RF02
    o.order_id,
    c.name AS customer_name,
    c.country,
    o.amount
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id;

-- Broadcast with multiple small tables in a single query
SELECT /*+ BROADCAST(c), BROADCAST(p) */ -- noqa: RF02
    o.order_id,
    c.name AS customer_name,
    p.name AS product_name,
    oi.qty,
    o.amount
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id
INNER JOIN order_items AS oi
    ON o.order_id = oi.order_id
INNER JOIN products AS p
    ON oi.product_id = p.product_id;

-- -----------------------------------------------------------------------
-- 2. MERGE hint — force SortMergeJoin
-- -----------------------------------------------------------------------

-- Best for: two large tables that are already sorted or partitioned on the
-- join key, or when data must be processed in sort order.
-- Both sides are sorted then merged without broadcasting either side.
SELECT /*+ MERGE(o) */ -- noqa: RF02
    o.order_id,
    c.name AS customer_name,
    o.amount
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id
ORDER BY o.order_id;

-- -----------------------------------------------------------------------
-- 3. SHUFFLE_HASH hint — hash-partition both sides before joining
-- -----------------------------------------------------------------------

-- Best for: tables that are too large to broadcast but one side is
-- significantly smaller and fits in memory after partitioning.
-- Avoids the sort step required by SortMergeJoin.
SELECT /*+ SHUFFLE_HASH(c) */ -- noqa: RF02
    o.order_id,
    c.name AS customer_name,
    o.amount
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id;

-- -----------------------------------------------------------------------
-- 4. SHUFFLE_REPLICATE_NL hint — nested-loop join without an equi key
-- -----------------------------------------------------------------------

-- Best for: non-equi joins or cross joins where no hash key is available.
-- One side is replicated to each partition; the other side loops through it.
-- Use sparingly — O(M×N) complexity.
SELECT /*+ SHUFFLE_REPLICATE_NL(c) */ -- noqa: RF02
    o.order_id,
    c.name AS customer_name,
    o.amount
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id <> c.customer_id  -- non-equi: no hash key possible
ORDER BY
    o.order_id,
    c.name;

-- -----------------------------------------------------------------------
-- 5. Hint on the right-hand table in a LEFT JOIN
-- -----------------------------------------------------------------------

-- The alias in the hint must match the table/subquery alias in the FROM clause.
SELECT /*+ BROADCAST(c) */ -- noqa: RF02
    o.order_id,
    o.amount,
    COALESCE(c.name, 'Unknown') AS customer_name
FROM orders AS o
LEFT JOIN customers AS c
    ON o.customer_id = c.customer_id;

-- -----------------------------------------------------------------------
-- 6. EXPLAIN to verify which join strategy Spark chose
-- -----------------------------------------------------------------------

-- Inspect the physical plan to confirm the hint was applied.
-- Look for "BroadcastHashJoin", "SortMergeJoin", or "ShuffledHashJoin" in
-- the output.
EXPLAIN
SELECT /*+ BROADCAST(c) */ -- noqa: RF02
    o.order_id,
    c.name AS customer_name
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id;
