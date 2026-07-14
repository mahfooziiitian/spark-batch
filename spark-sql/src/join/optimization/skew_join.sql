-- Skew join handling: detecting data skew, using the Databricks SKEW_JOIN hint,
-- and the manual salting technique for extreme skew cases.
--
-- What is skew?
--   Skew occurs when a small number of join-key values account for a
--   disproportionately large fraction of rows (e.g., customer_id = -1 for
--   all "guest" purchases).  One or a few tasks end up processing most of the
--   data while the rest finish quickly — causing stragglers and OOM errors.
--
-- Remedies (in order of preference):
--   1. SKEW_JOIN hint (Databricks Runtime ≥ 8.1 with AQE enabled):
--      Databricks Adaptive Query Execution (AQE) splits skewed partitions and
--      replicates the matching partition from the other side automatically.
--   2. Manual salting: add a random integer suffix to the key so a "hot" key
--      is spread across N buckets; replicate the other side N-fold to match.
--   3. BROADCAST the smaller table if it fits in memory after deduplication.
-- -----------------------------------------------------------------------

-- -----------------------------------------------------------------------
-- Test data
-- -----------------------------------------------------------------------

-- Large transaction table with heavy skew on customer_id = 0 (guest checkouts)
CREATE OR REPLACE TEMP VIEW txns AS
SELECT
    txn_id,
    customer_id,
    amount
FROM
    VALUES
    (1, 0, 10.00),  -- guest
    (2, 0, 20.00),  -- guest
    (3, 0, 30.00),  -- guest
    (4, 0, 40.00),  -- guest
    (5, 0, 50.00),  -- guest
    (6, 101, 60.00),
    (7, 102, 70.00),
    (8, 103, 80.00)
        AS txns (txn_id, customer_id, amount);

CREATE OR REPLACE TEMP VIEW customers AS
SELECT
    customer_id,
    name
FROM
    VALUES
    (0, 'Guest'),
    (101, 'Alice'),
    (102, 'Bob'),
    (103, 'Charlie')
        AS customers (customer_id, name);

-- -----------------------------------------------------------------------
-- 1. Detect skew — inspect the key distribution before joining
-- -----------------------------------------------------------------------

-- Count rows per join key; a hot key will have a much higher count than others.
SELECT
    customer_id,
    COUNT(*) AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM txns
GROUP BY customer_id
ORDER BY cnt DESC;
-- Result:
--   customer_id 0   → 5 rows (62.5 %) — clear skew hotspot
--   customer_id 101 → 1 row
--   customer_id 102 → 1 row
--   customer_id 103 → 1 row

-- -----------------------------------------------------------------------
-- 2. SKEW_JOIN hint (Databricks AQE)
-- -----------------------------------------------------------------------

-- Requires: spark.sql.adaptive.enabled = true (default in DBR 8.1+)
--            spark.sql.adaptive.skewJoin.enabled = true (default in DBR 8.1+)
--
-- The hint tells Spark which table has the skewed key and what the value is.
-- AQE will split the large partition and replicate the matching partition
-- from the other side automatically — no manual salting needed.
SELECT /*+ SKEW_JOIN(t, customer_id, (0)) */ -- noqa: RF02
    t.txn_id,
    c.name AS customer_name,
    t.amount
FROM txns AS t
INNER JOIN customers AS c
    ON t.customer_id = c.customer_id;
-- Result: 8 rows — same as a plain join but without the skewed straggler task

-- -----------------------------------------------------------------------
-- 3. Manual salting — spread a hot key across N buckets
-- -----------------------------------------------------------------------

-- Step A: Salt the large (skewed) table.
--   Append a random bucket number (0 to N-1) to the join key.
--   N = 4 in this example; increase N for more extreme skew.
WITH salted_transactions AS (
    SELECT
        txn_id,
        customer_id,
        amount,
        /* Assign a random bucket 0–3; CEIL(RAND()*N) - 1 gives 0..N-1 */
        CAST(FLOOR(RAND() * 4) AS INT) AS salt,
        CAST(customer_id AS STRING)
        || '_'
        || CAST(FLOOR(RAND() * 4) AS INT) AS salted_key
    FROM txns
),

b AS (
    SELECT EXPLODE(SEQUENCE(0, 3)) AS bucket_num  -- buckets 0, 1, 2, 3
),

replicated_customers AS (
    SELECT
        c.customer_id,
        c.name,
        b.bucket_num,
        CAST(c.customer_id AS STRING)
        || '_'
        || CAST(b.bucket_num AS INT) AS salted_key
    FROM customers AS c
    CROSS JOIN b
)

SELECT
    st.txn_id,
    rc.name AS customer_name,
    st.amount
FROM salted_transactions AS st
INNER JOIN replicated_customers AS rc
    ON st.salted_key = rc.salted_key;
-- Result: 8 rows, same as a plain join; skewed key 0 now spreads across
--         up to 4 tasks instead of landing in a single straggler partition.

-- -----------------------------------------------------------------------
-- 4. Salting with a deterministic salt for reproducible pipelines
-- -----------------------------------------------------------------------

-- When RAND() is undesirable (non-deterministic), use MOD(txn_id, N) as salt.
WITH salted_transactions AS (
    SELECT
        txn_id,
        customer_id,
        amount,
        (txn_id % 4) AS salt,
        CAST(customer_id AS STRING)
        || '_'
        || CAST(txn_id % 4 AS INT) AS salted_key
    FROM txns
),

b AS (
    SELECT EXPLODE(SEQUENCE(0, 3)) AS bucket_num
),

replicated_customers AS (
    SELECT
        c.customer_id,
        c.name,
        b.bucket_num,
        CAST(c.customer_id AS STRING)
        || '_'
        || CAST(b.bucket_num AS INT) AS salted_key
    FROM customers AS c
    CROSS JOIN b
)

SELECT
    st.txn_id,
    rc.name AS customer_name,
    st.amount
FROM salted_transactions AS st
INNER JOIN replicated_customers AS rc
    ON st.salted_key = rc.salted_key
ORDER BY st.txn_id;
-- Result: 8 rows, deterministic — the hot key 0 is evenly distributed
--         across 4 partitions using txn_id mod 4 as the salt.

-- -----------------------------------------------------------------------
-- 5. Broadcast as a simple remedy when the small table fits in memory
-- -----------------------------------------------------------------------

-- If customers is small enough to broadcast, this avoids the shuffle entirely
-- and is simpler than salting.
SELECT /*+ BROADCAST(c) */ -- noqa: RF02
    t.txn_id,
    c.name AS customer_name,
    t.amount
FROM txns AS t
INNER JOIN customers AS c
    ON t.customer_id = c.customer_id;
-- Result: 8 rows — skew is irrelevant because there is no shuffle at all
