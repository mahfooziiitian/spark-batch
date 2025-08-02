-- Databricks notebook source
-- Creating tables
CREATE TABLE IF NOT EXISTS poc.testing.employees (
    id INT,
    name STRING,
    hire_date DATE
);
-- Inserting data
INSERT INTO poc.testing.employees(id, name, hire_date)
VALUES (1, 'Alice', DATE('2022-01-15')),
    (2, 'Bob', CAST('2022-02-01' AS DATE));
-- Select data
SELECT *
FROM poc.testing.employees;

-- add or substract days from date
SELECT date_add('2025-07-01', 5);  -- Returns: 2025-07-06
SELECT date_sub(current_date(), 30); -- 30 days ago

-- drop tables
DROP TABLE IF EXISTS poc.testing.employees;