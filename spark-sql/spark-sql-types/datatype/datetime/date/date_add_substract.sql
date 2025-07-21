-- Databricks notebook source

-- add or substract days from date
SELECT date_add('2025-07-01', 5);  -- Returns: 2025-07-06
SELECT date_sub(current_date(), 30); -- 30 days ago
