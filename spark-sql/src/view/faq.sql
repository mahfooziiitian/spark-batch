-- ============================================================
-- Topic: Views — FAQ scenarios
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Shows how views behave when base table schemas change.
-- ============================================================

-- View uses outdated schema
DESCRIBE SCHEMA mgmt_stg.poc;
SHOW TABLES IN mgmt_stg.poc;

-- Step 1 - Create a table and a view
-- Create a sample table
CREATE OR REPLACE TABLE mgmt_stg.poc.sales (
    order_id INT,
    total DECIMAL(10, 2)
);

-- Create a view referencing a column
CREATE OR REPLACE VIEW mgmt_stg.poc.sales_view AS
SELECT
    order_id,
    total
FROM mgmt_stg.poc.sales;

SELECT * --noqa: AM04
FROM mgmt_stg.poc.sales_view;

-- Step 2 - Change the schema of the base table
-- Rename 'total' to 'amount'
ALTER TABLE mgmt_stg.poc.sales
    SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE mgmt_stg.poc.sales RENAME COLUMN total TO amount;

-- Step 3 - Query the view
SELECT * --noqa: AM04
FROM mgmt_stg.poc.sales_view;

-- View uses outdated schema
-- Step 1 - Create a table and a view
-- Create a sample table
DROP TABLE IF EXISTS mgmt_stg.poc.sales_all;

CREATE OR REPLACE TABLE mgmt_stg.poc.sales_all (
    order_id INT,
    total DECIMAL(10, 2)
);

-- Create a view referencing a column
CREATE OR REPLACE VIEW mgmt_stg.poc.sales_all_view AS
SELECT * --noqa: AM04
FROM mgmt_stg.poc.sales_all;

SELECT * --noqa: AM04
FROM mgmt_stg.poc.sales_all_view;

-- Step 2 - Change the schema of the base table
-- Rename 'total' to 'amount'
ALTER TABLE mgmt_stg.poc.sales_all
    SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE mgmt_stg.poc.sales_all RENAME COLUMN total TO amount;

-- Other scenario
CREATE OR REPLACE TABLE mgmt_stg.poc.sales_all (
    order_id INT,
    amount DECIMAL(10, 2)
);

-- Step 3 - Query the view
SELECT * --noqa: AM04
FROM mgmt_stg.poc.sales_all_view;

DESCRIBE TABLE mgmt_stg.poc.sales_all_view;

-- Fix
-- Schedule re-creation of view daily.

-- Clean up tables
DROP TABLE IF EXISTS mgmt_stg.poc.sales_all;
DROP VIEW IF EXISTS mgmt_stg.poc.sales_all_view;
DROP TABLE IF EXISTS mgmt_stg.poc.sales;
DROP VIEW IF EXISTS mgmt_stg.poc.sales_view;
