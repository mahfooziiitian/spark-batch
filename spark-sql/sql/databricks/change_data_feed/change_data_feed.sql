-- ============================================================
-- Topic: Change Data Feed (CDF) — row-level change tracking
-- Dialect: Databricks Runtime (Delta Lake) — NOT open-source Spark
-- Description: Read exactly which rows were inserted, updated, or deleted between
--   two table versions, without diffing full snapshots or re-scanning the whole table.
-- ============================================================

-- [Databricks] delta.enableChangeDataFeed and table_changes() are Databricks/Delta
-- Lake extensions with no open-source Spark equivalent.

---------------------------------------------------------------------------------------------------
-- Setup: a Delta table with CDF enabled at creation time
---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS customer_accounts;

-- [Databricks] Requires Delta Lake
CREATE TABLE customer_accounts (
    customer_id INT,
    tier STRING,
    balance DOUBLE
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = TRUE);

INSERT INTO customer_accounts VALUES
(1, 'silver', 100.0),
(2, 'gold', 500.0),
(3, 'silver', 50.0);

-- Version 1 (after the INSERT above) — CDF has nothing to report yet for reads
-- starting at version 1, since it captures changes *between* versions.

---------------------------------------------------------------------------------------------------
-- 1. Enable CDF on an existing table (if not set at creation time)
--    Only changes made *after* this ALTER TABLE are captured — history prior to
--    enabling CDF is not retroactively recorded.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake
ALTER TABLE customer_accounts SET TBLPROPERTIES (delta.enableChangeDataFeed = TRUE);

---------------------------------------------------------------------------------------------------
-- 2. Make some changes to generate a change feed: update, insert, delete
---------------------------------------------------------------------------------------------------

-- Version 2: upgrade customer 1 to gold and top up their balance
UPDATE customer_accounts
SET tier = 'gold', balance = balance + 25.0
WHERE customer_id = 1;

-- Version 3: a new customer signs up
INSERT INTO customer_accounts VALUES (4, 'silver', 20.0);

-- Version 4: customer 3 closes their account
DELETE FROM customer_accounts
WHERE customer_id = 3;

---------------------------------------------------------------------------------------------------
-- 3. Read the change feed by version range
--    table_changes(table, start_version, end_version) returns one row per change, with
--    three metadata columns appended: _change_type, _commit_version, _commit_timestamp.
--    _change_type is one of: insert, update_preimage, update_postimage, delete.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake with CDF enabled
SELECT
    customer_id,
    tier,
    balance,
    _change_type,
    _commit_version
FROM table_changes('customer_accounts', 2, 4)
ORDER BY _commit_version, customer_id;

-- Result includes:
--   version 2: update_preimage  (1, silver, 100.0) and update_postimage (1, gold, 125.0)
--   version 3: insert           (4, silver,  20.0)
--   version 4: delete           (3, silver,  50.0)

---------------------------------------------------------------------------------------------------
-- 4. Read the change feed by timestamp range
--    Equivalent to the version-based form, but expressed in wall-clock time — useful
--    for "changes since last run" incremental pipelines driven by a schedule.
---------------------------------------------------------------------------------------------------

-- [Databricks] Requires Delta Lake with CDF enabled
SELECT
    customer_id,
    tier,
    balance,
    _change_type,
    _commit_timestamp
FROM table_changes('customer_accounts', '2024-01-01T00:00:00', current_timestamp())
ORDER BY _commit_timestamp, customer_id;

---------------------------------------------------------------------------------------------------
-- 5. Only the "final" state of each change (net inserts/updates, ignoring preimages)
--    Filters out update_preimage rows, keeping only the post-change row for updates —
--    the typical shape needed to propagate net changes downstream (e.g. into a MERGE
--    against a reporting table).
---------------------------------------------------------------------------------------------------

SELECT
    customer_id,
    tier,
    balance,
    _change_type,
    _commit_version
FROM table_changes('customer_accounts', 2, 4)
WHERE _change_type != 'update_preimage'
ORDER BY _commit_version, customer_id;

-- Result: one net row per change (insert/update_postimage/delete), ready to drive a
-- downstream MERGE INTO that applies the same inserts/updates/deletes elsewhere.
