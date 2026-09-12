-- ============================================================
-- Topic: Table cloning — SHALLOW CLONE and DEEP CLONE
-- Dialect: Databricks Runtime (Delta Lake) — NOT open-source Spark
-- Description: Create a copy of a Delta table for testing, backup, or point-in-time
--   snapshotting, either by referencing the source's existing data files (shallow) or
--   physically copying them (deep).
-- ============================================================

-- [Databricks] SHALLOW CLONE / DEEP CLONE are Databricks Delta Lake extensions with
-- no open-source Spark equivalent.

---------------------------------------------------------------------------------------------------
-- Setup: source Delta table
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_product;

CREATE TABLE dim_product (
    product_id INT,
    name       STRING,
    category   STRING,
    price      DOUBLE
) USING DELTA;

INSERT INTO dim_product VALUES
    (1, 'Widget A', 'Tools',       9.99),
    (2, 'Widget B', 'Tools',      19.99),
    (3, 'Gadget X', 'Electronics', 49.99);

-- Version 0 established above; later versions created below to demonstrate
-- version-pinned clones.
UPDATE dim_product SET price = 12.99 WHERE product_id = 1;
INSERT INTO dim_product VALUES (4, 'Gadget Y', 'Electronics', 89.99);

---------------------------------------------------------------------------------------------------
-- 1. SHALLOW CLONE: metadata-only copy
--    Copies the Delta transaction log but not the underlying data files — the clone
--    references the source's existing Parquet files. Fast and cheap (no data movement),
--    ideal for spinning up a dev/test copy of a large table. The clone is writable, but
--    writes to the clone never modify the source and vice versa. If the source's files
--    are later VACUUMed the clone can lose access to that data, so shallow clones are
--    best for short-lived experimentation rather than long-term backups.
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_product_dev;

-- [Databricks] Requires Delta Lake
CREATE TABLE dim_product_dev
SHALLOW CLONE dim_product;

-- The dev clone starts identical to the source's current version, and can be modified
-- independently for testing without touching production data:
UPDATE dim_product_dev SET price = 999.99 WHERE product_id = 1;

SELECT product_id, price FROM dim_product_dev WHERE product_id = 1;
-- Result: 999.99 (dev clone only)
SELECT product_id, price FROM dim_product WHERE product_id = 1;
-- Result: 12.99 (source untouched)

---------------------------------------------------------------------------------------------------
-- 2. DEEP CLONE: full physical copy
--    Copies both the transaction log and every data file into the clone's own storage
--    location. Slower and storage-heavier than SHALLOW CLONE, but the clone is fully
--    independent — safe as a durable backup, or as a snapshot that must survive the
--    source table being VACUUMed or dropped entirely.
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_product_backup;

-- [Databricks] Requires Delta Lake
CREATE TABLE dim_product_backup
DEEP CLONE dim_product;

---------------------------------------------------------------------------------------------------
-- 3. Clone a specific historical version (time travel + clone)
--    Combines Delta time travel with cloning to durably capture a point-in-time
--    snapshot — e.g. before a risky bulk UPDATE/MERGE, or for month-end reporting.
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_product_asof_v0;

-- [Databricks] Requires Delta Lake
CREATE TABLE dim_product_asof_v0
DEEP CLONE dim_product VERSION AS OF 0;

-- Result: dim_product_asof_v0 has the original 3 rows with product 1 still priced at
-- 9.99 — the price update and product 4 insert (versions 1-2) are not included.

-- Equivalent using a timestamp instead of a version number:
-- CREATE TABLE dim_product_asof_ts DEEP CLONE dim_product TIMESTAMP AS OF '2024-06-01';

---------------------------------------------------------------------------------------------------
-- 4. Incrementally refresh an existing clone
--    Re-running CREATE ... CLONE with CREATE OR REPLACE against an existing clone only
--    copies the files that changed since the clone was last created/refreshed, instead
--    of a full re-copy — useful for keeping a nightly backup or staging replica in sync.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake
CREATE OR REPLACE TABLE dim_product_backup
DEEP CLONE dim_product;

-- Result: dim_product_backup now matches the current (4-row) state of dim_product;
-- only files added/changed since the previous clone are physically copied.
