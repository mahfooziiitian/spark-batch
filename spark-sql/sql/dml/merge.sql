-- [Databricks] MERGE INTO (upsert) examples for Delta Lake
-- MERGE is the recommended way to perform upserts, conditional updates, and full-sync patterns
-- on Delta tables.  The source is scanned once; matched/unmatched rows are routed to their
-- respective WHEN clauses in a single pass.
-- Uses Databricks/Delta-exclusive syntax (WHEN NOT MATCHED BY SOURCE, column DEFAULT values,
-- schema auto-merge) not parseable by the open-source `sparksql` dialect — see .sqlfluffignore.

---------------------------------------------------------------------------------------------------
-- Setup: target and source tables
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS products;

CREATE TABLE products (
    product_id INT,
    name STRING,
    category STRING,
    price DOUBLE,
    stock INT,
    updated_at TIMESTAMP
) USING DELTA;

INSERT INTO products VALUES
(1, 'Widget A', 'Tools', 9.99, 100, CURRENT_TIMESTAMP()),
(2, 'Widget B', 'Tools', 19.99, 50, CURRENT_TIMESTAMP()),
(3, 'Gadget X', 'Electronics', 49.99, 30, CURRENT_TIMESTAMP()),
(4, 'Gadget Y', 'Electronics', 89.99, 20, CURRENT_TIMESTAMP()),
(5, 'Doohickey', 'Misc', 4.99, 200, CURRENT_TIMESTAMP());

-- Incoming feed: contains updates to existing products and one new product
CREATE OR REPLACE TEMP VIEW incoming_products AS
SELECT
    product_id,
    name,
    category,
    price,
    stock
FROM
    VALUES
    (1, 'Widget A', 'Tools', 11.99, 120),   -- price/stock changed
    (3, 'Gadget X', 'Electronics', 49.99, 30),   -- unchanged
    (6, 'Sprocket', 'Hardware', 7.49, 75)    -- new product
        AS t (product_id, name, category, price, stock);

---------------------------------------------------------------------------------------------------
-- 1. Basic upsert: UPDATE matched rows, INSERT new rows
---------------------------------------------------------------------------------------------------

MERGE INTO products AS t
USING incoming_products AS s
    ON t.product_id = s.product_id
WHEN MATCHED THEN
    UPDATE SET
        t.name = s.name,
        t.category = s.category,
        t.price = s.price,
        t.stock = s.stock,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (product_id, name, category, price, stock, updated_at)
    VALUES (s.product_id, s.name, s.category, s.price, s.stock, CURRENT_TIMESTAMP());

-- Result: product 1 updated, product 3 updated (even though unchanged), product 6 inserted

---------------------------------------------------------------------------------------------------
-- 2. Conditional update: only update if at least one value actually changed
---------------------------------------------------------------------------------------------------

MERGE INTO products AS t
USING incoming_products AS s
    ON t.product_id = s.product_id
WHEN MATCHED AND (t.price <> s.price OR t.stock <> s.stock OR t.name <> s.name) THEN
    UPDATE SET
        t.name = s.name,
        t.price = s.price,
        t.stock = s.stock,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (product_id, name, category, price, stock, updated_at)
    VALUES (s.product_id, s.name, s.category, s.price, s.stock, CURRENT_TIMESTAMP());

-- Result: product 3 (unchanged) is skipped; only genuinely changed rows are written

---------------------------------------------------------------------------------------------------
-- 3. Upsert + delete: three-clause MERGE
--    Any product in the target that is marked as 'discontinued' in the source is deleted.
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW incoming_with_flag AS
SELECT
    product_id,
    name,
    category,
    price,
    stock,
    is_discontinued
FROM
    VALUES
    (2, 'Widget B', 'Tools', 19.99, 50, TRUE),   -- mark for deletion
    (6, 'Sprocket', 'Hardware', 7.49, 75, FALSE)    -- normal upsert
        AS t (product_id, name, category, price, stock, is_discontinued);

MERGE INTO products AS t
USING incoming_with_flag AS s
    ON t.product_id = s.product_id
WHEN MATCHED AND s.is_discontinued = TRUE THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET
        t.price = s.price,
        t.stock = s.stock,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (product_id, name, category, price, stock, updated_at)
    VALUES (s.product_id, s.name, s.category, s.price, s.stock, CURRENT_TIMESTAMP());

-- Result: product 2 deleted; product 6 inserted (or updated if already present)

---------------------------------------------------------------------------------------------------
-- 4. WHEN NOT MATCHED BY SOURCE THEN DELETE (full-sync / mirror pattern)
--    Any row in the target that has NO matching row in the source is removed.
--    Use this to keep the target a perfect mirror of the source.
---------------------------------------------------------------------------------------------------

MERGE INTO products AS t
USING incoming_products AS s
    ON t.product_id = s.product_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;

-- Result: target ends up containing exactly the rows in incoming_products

---------------------------------------------------------------------------------------------------
-- 5. Merge from an aggregated subquery
--    Aggregate the source before merging to avoid "multiple rows matched" errors.
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW order_items AS
SELECT
    product_id,
    qty_sold
FROM
    VALUES
    (1, 5), (1, 3), (3, 10), (6, 2)
        AS t (product_id, qty_sold);

MERGE INTO products AS t
USING (
    SELECT
        product_id,
        SUM(qty_sold) AS total_sold
    FROM order_items
    GROUP BY product_id
) AS s
    ON t.product_id = s.product_id
WHEN MATCHED THEN
    UPDATE SET
        t.stock = t.stock - s.total_sold,
        t.updated_at = CURRENT_TIMESTAMP();

-- Result: stock for products 1 and 3 is decremented by total units sold

---------------------------------------------------------------------------------------------------
-- 6. SCD Type 1 upsert using MERGE
--    Overwrites history — the target always reflects the latest source values.
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING,
    updated_at TIMESTAMP
) USING DELTA;

CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT
    customer_id,
    name,
    email,
    city
FROM
    VALUES
    (1, 'Alice', 'alice@example.com', 'Boston'),
    (2, 'Bob', 'bob@new.com', 'Seattle'),
    (3, 'Carol', 'carol@example.com', 'Miami')
        AS t (customer_id, name, email, city);

MERGE INTO dim_customer AS t
USING staging_customer AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET
        t.name = s.name,
        t.email = s.email,
        t.city = s.city,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, updated_at)
    VALUES (s.customer_id, s.name, s.email, s.city, CURRENT_TIMESTAMP());

-- Result: existing customers updated in place; new customers inserted; no history kept

---------------------------------------------------------------------------------------------------
-- 7. Deduplicate source before MERGE (ROW_NUMBER CTE pattern)
--    If the source can contain multiple rows per key, deduplicate first to avoid
--    the "MERGE into Delta table ... multiple source rows matched a single target row"
--    runtime error.
---------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW raw_feed AS
SELECT
    product_id,
    name,
    price,
    stock,
    arrived_at
FROM
    VALUES
    (1, 'Widget A', 11.99, 120, TIMESTAMP '2024-03-01 09:00:00'),
    (1, 'Widget A', 11.99, 125, TIMESTAMP '2024-03-01 10:00:00'),   -- duplicate key, later ts
    (6, 'Sprocket', 7.49, 75, TIMESTAMP '2024-03-01 08:00:00')
        AS t (product_id, name, price, stock, arrived_at);

WITH deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY arrived_at DESC) AS rn
        FROM raw_feed
    ) AS ranked
    WHERE rn = 1
)

MERGE INTO products AS t
USING deduped AS s
    ON t.product_id = s.product_id
WHEN MATCHED THEN
    UPDATE SET
        t.name = s.name,
        t.price = s.price,
        t.stock = s.stock,
        t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (product_id, name, price, stock, updated_at)
    VALUES (s.product_id, s.name, s.price, s.stock, CURRENT_TIMESTAMP());

-- Result: only the latest row per product_id is used; no "multiple rows matched" error

---------------------------------------------------------------------------------------------------
-- 8. Soft delete: WHEN NOT MATCHED BY SOURCE THEN UPDATE (mark inactive, keep the row)
--    Unlike example 4, this preserves history instead of physically deleting rows —
--    the preferred approach when downstream consumers still need to see retired products.
---------------------------------------------------------------------------------------------------

ALTER TABLE products ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE; -- noqa: PRS

MERGE INTO products AS t
USING incoming_products AS s
    ON t.product_id = s.product_id
WHEN MATCHED THEN
    UPDATE SET t.stock = s.stock, t.updated_at = CURRENT_TIMESTAMP(), t.is_active = TRUE
WHEN NOT MATCHED THEN
    INSERT (product_id, name, category, price, stock, updated_at, is_active)
    VALUES (s.product_id, s.name, s.category, s.price, s.stock, CURRENT_TIMESTAMP(), TRUE)
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET t.is_active = FALSE, t.updated_at = CURRENT_TIMESTAMP();

-- Result: products absent from incoming_products are flagged is_active = FALSE instead of
-- being deleted — reporting queries can filter WHERE is_active, while audits keep full history.

---------------------------------------------------------------------------------------------------
-- 9. Idempotent event merge: apply each event exactly once
--    Streaming/CDC sources can redeliver the same event_id after a retry or reprocess.
--    Guarding the INSERT condition on event_id prevents duplicate application.
---------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS product_stock_ledger (
    product_id INT,
    event_id STRING,
    qty_delta INT,
    applied_at TIMESTAMP
) USING DELTA;

CREATE OR REPLACE TEMP VIEW incoming_events AS
SELECT
    product_id,
    event_id,
    qty_delta
FROM
    VALUES
    (1, 'evt-1001', -5),
    (3, 'evt-1002', -2),
    (1, 'evt-1001', -5)   -- redelivered duplicate of evt-1001, must not double-apply
        AS t (product_id, event_id, qty_delta);

MERGE INTO product_stock_ledger AS t
USING (SELECT DISTINCT
    product_id,
    event_id,
    qty_delta
FROM incoming_events) AS s
    ON t.event_id = s.event_id
WHEN NOT MATCHED THEN
    INSERT (product_id, event_id, qty_delta, applied_at)
    VALUES (s.product_id, s.event_id, s.qty_delta, CURRENT_TIMESTAMP());

-- Result: evt-1001 is recorded once even though it appeared twice in the source batch —
-- MERGE ON event_id makes re-running this job on a redelivered/replayed batch a safe no-op.

---------------------------------------------------------------------------------------------------
-- 10. Schema evolution: source has a new column not yet in the target
--    Requires spark.databricks.delta.schema.autoMerge.enabled = true (session or table property).
---------------------------------------------------------------------------------------------------

SET spark.databricks.delta.schema.automerge.enabled = TRUE;

CREATE OR REPLACE TEMP VIEW incoming_products_v2 AS
SELECT
    product_id,
    name,
    category,
    price,
    stock,
    supplier AS string
FROM
    VALUES
    (1, 'Widget A', 'Tools', 11.99, 120, 'Acme Corp'),
    (7, 'Wrench', 'Tools', 6.49, 60, 'Acme Corp')
        AS t (product_id, name, category, price, stock, supplier);

MERGE INTO products AS t
USING incoming_products_v2 AS s
    ON t.product_id = s.product_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *;

-- Result: the target table gains a `supplier` column automatically; existing rows get NULL
-- supplier until their next update. Without autoMerge this MERGE fails with a schema mismatch.

---------------------------------------------------------------------------------------------------
-- 11. Performance: prune the target with a static partition filter in the ON clause
--    Without a partition predicate, Spark scans every target file to find matches.
---------------------------------------------------------------------------------------------------

-- Bad: scans the entire (potentially huge) target table
-- MERGE INTO events_by_day AS t USING daily_batch AS s ON t.event_id = s.event_id ...

-- Good: partition column repeated in the ON clause enables partition pruning on the target
MERGE INTO products AS t
USING incoming_products AS s
    ON
        t.product_id = s.product_id
        AND t.category IN ('Tools', 'Electronics', 'Misc', 'Hardware')   -- static predicate, prunable
WHEN MATCHED THEN
    UPDATE SET t.price = s.price, t.stock = s.stock, t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (product_id, name, category, price, stock, updated_at)
    VALUES (s.product_id, s.name, s.category, s.price, s.stock, CURRENT_TIMESTAMP());

-- Result: identical rows to example 1, but if `products` were partitioned by `category`,
-- Spark would only scan the listed partitions instead of the whole table.

---------------------------------------------------------------------------------------------------
-- 12. Common MERGE pitfalls
---------------------------------------------------------------------------------------------------

-- | Pitfall                                     | Fix                                              |
-- |------------------------------------------------|------------------------------------------------------|
-- | Source has duplicate keys                      | Deduplicate first (see example 7) — MERGE raises  |
-- |                                                  | a runtime error if >1 source row matches 1 target |
-- | Forgetting a partition predicate on large target| Add a static filter to ON clause (see example 11) |
-- | New source column but autoMerge disabled        | SET spark.databricks.delta.schema.autoMerge        |
-- |                                                  | .enabled = true                                    |
-- | Physically deleting rows still needed downstream| Use NOT MATCHED BY SOURCE THEN UPDATE for a soft  |
-- |                                                  | delete instead of THEN DELETE (see example 8)     |
-- | Re-running a job re-applies the same event twice| Key the MERGE ON a unique event/message id        |
-- |                                                  | (see example 9)                                    |
-- | UPDATE SET * silently drops columns not in src  | List columns explicitly, or ensure source has     |
-- |                                                  | every target column (schema evolution, example 10)|
