-- Comparison condition examples in Spark SQL (Databricks).
-- Covers =, !=, >, <, >=, <=, BETWEEN, IN, NOT IN, NULL-safe <=>,
-- NULL comparison behaviour, date ranges, and IN with a subquery.

-- ----------------------------------------------------------------------------
-- Setup: products table
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW products AS
SELECT
    product_id,
    product_name,
    category,
    price,
    stock,
    launch_date
FROM
    VALUES
    (1, 'Laptop', 'Electronics', 999.99, 50, DATE '2023-03-15'),
    (2, 'Desk', 'Furniture', 299.00, 120, DATE '2022-11-01'),
    (3, 'Monitor', 'Electronics', 399.50, 80, DATE '2023-06-20'),
    (4, 'Chair', 'Furniture', 149.00, 200, DATE '2021-07-10'),
    (5, 'Headphones', 'Electronics', 79.99, 300, DATE '2024-01-05'),
    (6, 'Keyboard', 'Electronics', 49.99, NULL, DATE '2024-02-14')
        AS t (product_id, product_name, category, price, stock, launch_date);

-- ----------------------------------------------------------------------------
-- 1. Basic comparison operators: =, !=, >, <, >=, <=
-- ----------------------------------------------------------------------------
SELECT
    product_name,
    price
FROM products
WHERE price = 299.00;           -- exact match
-- Result: Desk

SELECT
    product_name,
    price
FROM products
WHERE price != 999.99;          -- not equal (also written <>)
-- Result: all except Laptop

SELECT
    product_name,
    price
FROM products
WHERE price > 300.00;           -- greater than
-- Result: Laptop (999.99), Monitor (399.50)

SELECT
    product_name,
    price
FROM products
WHERE price < 100.00;           -- less than
-- Result: Headphones (79.99), Keyboard (49.99)

SELECT
    product_name,
    stock
FROM products
WHERE stock >= 200;             -- greater than or equal
-- Result: Chair (200), Headphones (300)

SELECT
    product_name,
    price
FROM products
WHERE price <= 299.00;          -- less than or equal
-- Result: Desk (299.00), Headphones (79.99), Keyboard (49.99), Chair (149.00)

-- ----------------------------------------------------------------------------
-- 2. BETWEEN: inclusive range (equivalent to >= AND <=)
-- ----------------------------------------------------------------------------
SELECT
    product_name,
    price
FROM products
WHERE price BETWEEN 100.00 AND 400.00;
-- Result: Desk (299.00), Monitor (399.50), Chair (149.00)

-- ----------------------------------------------------------------------------
-- 3. NOT BETWEEN
-- ----------------------------------------------------------------------------
SELECT
    product_name,
    price
FROM products
WHERE price NOT BETWEEN 100.00 AND 400.00;
-- Result: Laptop (999.99), Headphones (79.99), Keyboard (49.99)

-- ----------------------------------------------------------------------------
-- 4. IN list
-- ----------------------------------------------------------------------------
SELECT
    product_name,
    category
FROM products
WHERE category IN ('Electronics', 'Furniture');
-- Result: all rows (both categories present)

SELECT
    product_name,
    product_id
FROM products
WHERE product_id IN (1, 3, 5);
-- Result: Laptop, Monitor, Headphones

-- ----------------------------------------------------------------------------
-- 5. NOT IN list
-- ----------------------------------------------------------------------------
-- ⚠ NULL trap: if the IN list or subquery contains NULL, NOT IN returns no rows
--   for any value that does not match — because NULL comparison is unknown.
--   Always ensure the list/subquery is NULL-free.
SELECT
    product_name,
    category
FROM products
WHERE category NOT IN ('Furniture');
-- Result: Electronics products only (Laptop, Monitor, Headphones, Keyboard)

-- Demonstrating the NULL trap: stock has one NULL row (Keyboard).
-- NOT IN (50, 80, NULL) returns NO rows — the NULL poisons the comparison.
SELECT
    product_name,
    stock
FROM products
-- Result: empty — NULL in list blocks all rows
WHERE stock NOT IN (50, 80, NULL);

-- Safe pattern: exclude NULLs from the list or add IS NOT NULL guard.
SELECT
    product_name,
    stock
FROM products
WHERE
    stock NOT IN (50, 80)
    AND stock IS NOT NULL;
-- Result: Chair (200), Headphones (300)

-- ----------------------------------------------------------------------------
-- 6. NULL-safe equality <=>
-- ----------------------------------------------------------------------------
-- Returns TRUE when both sides are NULL (unlike =).
SELECT
    product_name,
    stock,
    stock <=> NULL AS stock_is_null  -- TRUE only for Keyboard
FROM products;
-- Result: Keyboard → TRUE; all others → FALSE.

-- ----------------------------------------------------------------------------
-- 7. Comparison with NULL shows result is NULL (not TRUE or FALSE)
-- ----------------------------------------------------------------------------
SELECT
    product_name,
    stock,
    stock IS NULL AS eq_null,    -- always NULL, never TRUE
    stock > 0 AS gt_zero        -- NULL for Keyboard (NULL > 0 is NULL)
FROM products;
-- Result: Keyboard gets NULL for both eq_null and gt_zero.

-- ----------------------------------------------------------------------------
-- 8. BETWEEN with dates
-- ----------------------------------------------------------------------------
SELECT
    product_name,
    launch_date
FROM products
WHERE launch_date BETWEEN DATE '2023-01-01' AND DATE '2023-12-31';
-- Result: Laptop (2023-03-15), Monitor (2023-06-20)

-- ----------------------------------------------------------------------------
-- 9. IN with subquery
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW featured_categories AS
SELECT category
FROM
    VALUES ('Electronics')
        AS t (category);

SELECT
    p.product_name,
    p.category,
    p.price
FROM products AS p
WHERE p.category IN (
    SELECT fc.category
    FROM featured_categories AS fc
);
-- Result: all Electronics products (Laptop, Monitor, Headphones, Keyboard)
