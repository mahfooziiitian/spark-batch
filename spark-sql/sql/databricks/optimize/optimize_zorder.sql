-- ============================================================
-- Topic: Table optimization — OPTIMIZE, ZORDER BY, VACUUM
-- Dialect: Databricks Runtime (Delta Lake) — NOT open-source Spark
-- Description: Compact small files and co-locate data by high-cardinality
--   predicate columns after bulk writes (batch loads, SCD merges), then
--   reclaim storage from old file versions.
-- ============================================================

-- [Databricks] Requires Delta Lake. OPTIMIZE/ZORDER/VACUUM are Databricks
-- Runtime extensions with no open-source Spark equivalent.

---------------------------------------------------------------------------------------------------
-- Setup: a table written in several small batches (simulating streaming/incremental loads)
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS events;

CREATE TABLE events (
    event_id BIGINT,
    customer_id INT,
    event_type STRING,
    event_date DATE,
    payload STRING
) USING DELTA
PARTITIONED BY (event_date);

-- Each INSERT below produces its own small Parquet file(s) per partition, mirroring
-- how micro-batch/streaming writes fragment a table over time.
INSERT INTO events VALUES (1, 101, 'click', DATE '2024-06-01', 'home_page');
INSERT INTO events VALUES (2, 102, 'click', DATE '2024-06-01', 'search');
INSERT INTO events VALUES (3, 101, 'purchase', DATE '2024-06-01', 'checkout');
INSERT INTO events VALUES (4, 103, 'click', DATE '2024-06-02', 'home_page');
INSERT INTO events VALUES (5, 101, 'click', DATE '2024-06-02', 'profile');

---------------------------------------------------------------------------------------------------
-- 1. Basic OPTIMIZE: compact small files into fewer, larger files
--    Run after bulk writes (batch loads, SCD merges) to counter "small file problem"
--    caused by many narrow INSERT/MERGE operations.
---------------------------------------------------------------------------------------------------

-- [Databricks] Bin-packing compaction — no ZORDER, just file-size consolidation
OPTIMIZE events;

---------------------------------------------------------------------------------------------------
-- 2. OPTIMIZE ... ZORDER BY: co-locate rows by a high-cardinality predicate column
--    Z-ordering clusters related data within the same set of files, so predicates on
--    the Z-ORDER column can skip many files entirely (data skipping via file-level
--    min/max statistics). Best on columns frequently used in WHERE/JOIN, with high
--    cardinality (customer_id here has far more distinct values than event_type).
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake
OPTIMIZE events
ZORDER BY (customer_id);

-- A query filtering on customer_id can now skip files that don't contain it:
SELECT
    event_id,
    event_type,
    payload
FROM events
WHERE customer_id = 101
ORDER BY event_id;

-- Result: 3 rows (event_id 1, 3, 5) — file pruning is transparent to the query itself,
-- only the physical layout (and therefore scan cost) changes.

---------------------------------------------------------------------------------------------------
-- 3. OPTIMIZE with a WHERE predicate: limit compaction to specific partitions
--    Useful for large tables where only recently-written partitions need compaction,
--    avoiding rewriting the entire (already-optimized) history.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake; predicate must be on partition columns
OPTIMIZE events
WHERE event_date = DATE '2024-06-02'
ZORDER BY (customer_id);

---------------------------------------------------------------------------------------------------
-- 4. Inspect file layout via DESCRIBE DETAIL
--    numFiles drops after OPTIMIZE compacts small files into fewer, larger ones.
---------------------------------------------------------------------------------------------------

SELECT
    numFiles, -- noqa: CP02
    sizeInBytes, -- noqa: CP02
    partitionColumns -- noqa: CP02
FROM (DESCRIBE DETAIL events); -- noqa: PRS

---------------------------------------------------------------------------------------------------
-- 5. VACUUM: physically delete files no longer referenced by the current table version
--    OPTIMIZE does not delete the pre-compaction files immediately — Delta keeps them
--    around to support time travel. VACUUM removes files older than the retention
--    threshold once they are no longer needed.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake. Default retention is 7 days (168 hours); going
-- below that risks breaking concurrent readers/time-travel queries and requires
-- explicitly disabling the safety check.
VACUUM events RETAIN 168 HOURS;

-- Result: files from superseded table versions older than the retention window are
-- deleted from storage; the current table contents and schema are unaffected.
