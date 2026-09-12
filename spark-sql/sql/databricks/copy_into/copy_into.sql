-- ============================================================
-- Topic: Idempotent file ingestion — COPY INTO
-- Dialect: Databricks Runtime — NOT open-source Spark
-- Description: Incrementally and idempotently load new files from cloud storage
--   into a Delta table, without a streaming job or manual "already loaded" tracking.
-- ============================================================

-- [Databricks] COPY INTO is a Databricks SQL command with no open-source Spark
-- equivalent (Spark's own batch reader has no built-in idempotent-load tracking).

---------------------------------------------------------------------------------------------------
-- Setup: target Delta table
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS raw_orders;

CREATE TABLE raw_orders (
    order_id    INT,
    customer_id INT,
    amount      DOUBLE,
    order_date  DATE
) USING DELTA;

---------------------------------------------------------------------------------------------------
-- 1. Basic COPY INTO: load every new file once
--    Databricks tracks which source files have already been loaded into this table
--    (via the Delta transaction log), so re-running the exact same COPY INTO after
--    files were already ingested is a no-op — safe to schedule on a recurring job
--    without deduplication logic.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake target + cloud storage source
COPY INTO raw_orders
FROM '/mnt/landing/orders/'
FILEFORMAT = PARQUET;

-- Result: every file under the path not previously loaded is appended; running this
-- statement again with no new files is a no-op (0 rows copied).

---------------------------------------------------------------------------------------------------
-- 2. COPY INTO with explicit format options and schema evolution
--    'mergeSchema' lets new source columns be added to the target table automatically,
--    instead of failing on a schema mismatch — useful when upstream producers add
--    fields over time (e.g. a new `promo_code` column appears in later files).
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake
COPY INTO raw_orders
FROM '/mnt/landing/orders/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');

---------------------------------------------------------------------------------------------------
-- 3. COPY INTO from CSV with format-specific reader options
--    FORMAT_OPTIONS mirrors the DataFrameReader options available for the chosen
--    FILEFORMAT (header row, delimiter, permissive parsing, etc.).
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake
COPY INTO raw_orders
FROM '/mnt/landing/orders_csv/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header'    = 'true',
    'delimiter' = ',',
    'mode'      = 'PERMISSIVE'
)
COPY_OPTIONS ('mergeSchema' = 'true');

---------------------------------------------------------------------------------------------------
-- 4. COPY INTO with an explicit file pattern and per-column mapping
--    PATTERN restricts which files under the path are considered (e.g. only a single
--    day's partition of arriving files); SELECT lets the source columns be reordered,
--    renamed, or cast without a separate staging step.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake
COPY INTO raw_orders
FROM (
    SELECT
        order_id,
        customer_id,
        CAST(amount AS DOUBLE) AS amount,
        CAST(order_date AS DATE) AS order_date
    FROM '/mnt/landing/orders/'
)
FILEFORMAT = PARQUET
PATTERN = '2024-06-*.parquet';

---------------------------------------------------------------------------------------------------
-- 5. Force a reload of previously-copied files
--    Rarely needed (breaks the idempotency guarantee for the affected files) — use only
--    to recover from a source data correction that requires reprocessing the same files.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake
COPY INTO raw_orders
FROM '/mnt/landing/orders/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('force' = 'true');

-- Result: every matching file is re-loaded regardless of prior load history, so this
-- can introduce duplicate rows unless the target enforces uniqueness (e.g. via MERGE
-- instead of COPY INTO, or a downstream deduplication step).
