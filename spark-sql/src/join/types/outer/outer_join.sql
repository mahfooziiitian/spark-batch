-- Outer join examples: LEFT OUTER JOIN, RIGHT OUTER JOIN, FULL OUTER JOIN.
-- Covers basic patterns, NULLs introduced by outer joins,
-- COALESCE to fill NULLs,
-- and the anti-join pattern (LEFT JOIN + WHERE right.key IS NULL).
-- Tables: orders(order_id, customer_id, amount, status),
--         customers(customer_id, name, country)

-- -----------------------------------------------------------------------
-- Test data
-- -----------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW orders AS
SELECT
    order_id,
    customer_id,
    amount,
    status
FROM
    VALUES
    (1, 101, 250.00, 'completed'),
    (2, 102, 80.00, 'completed'),
    (3, 101, 430.00, 'pending'),
    (4, 103, 120.00, 'completed'),
    (5, 199, 960.00, 'cancelled')  -- customer 199 does NOT exist in customers
        AS orders (order_id, customer_id, amount, status);

CREATE OR REPLACE TEMP VIEW customers AS
SELECT
    customer_id,
    name,
    country
FROM
    VALUES
    (101, 'Alice', 'US'),
    (102, 'Bob', 'CA'),
    (103, 'Charlie', 'US'),
    (104, 'Diana', 'UK')  -- customer 104 has NO orders
        AS customers (customer_id, name, country);

-- -----------------------------------------------------------------------
-- 1. LEFT OUTER JOIN — all orders, with customer info where available
-- -----------------------------------------------------------------------

-- Every order is kept.  When no matching customer exists the customer
-- columns are NULL (see order_id 5, customer_id 199).
SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,  -- NULL for unmatched rows
    c.country,
    o.amount,
    o.status
FROM orders AS o
LEFT OUTER JOIN customers AS c
    ON o.customer_id = c.customer_id;
-- Result: 5 rows; order_id 5 → customer_name NULL, country NULL

-- -----------------------------------------------------------------------
-- 2. COALESCE to replace NULLs introduced by LEFT JOIN
-- -----------------------------------------------------------------------

SELECT
    o.order_id,
    o.amount,
    COALESCE(c.name, 'Unknown') AS customer_name,
    COALESCE(c.country, 'N/A') AS country
FROM orders AS o
LEFT OUTER JOIN customers AS c
    ON o.customer_id = c.customer_id;
-- Result: order_id 5 → customer_name 'Unknown', country 'N/A'

-- -----------------------------------------------------------------------
-- 3. RIGHT OUTER JOIN — all customers, with their orders where available
-- -----------------------------------------------------------------------

-- Every customer is kept.  When a customer has no orders the order
-- columns are NULL (see customer_id 104 / Diana).
SELECT
    c.customer_id,
    c.name AS customer_name,
    c.country,
    o.order_id,   -- NULL for customers with no orders
    o.amount,
    o.status
FROM orders AS o
RIGHT OUTER JOIN customers AS c  -- noqa: CV08
    ON o.customer_id = c.customer_id;
-- Result: 5 rows; Diana → order_id NULL, amount NULL, status NULL

-- -----------------------------------------------------------------------
-- 4. FULL OUTER JOIN — all orders and all customers, NULLs on both sides
-- -----------------------------------------------------------------------

-- Keeps every row from both tables.
-- Unmatched orders get NULL customer columns;
-- unmatched customers get NULL order columns.
SELECT
    COALESCE(o.order_id, -1) AS order_id,
    COALESCE(c.customer_id, o.customer_id) AS customer_id,
    COALESCE(c.name, 'Unknown') AS customer_name,
    COALESCE(o.amount, 0.00) AS amount,
    COALESCE(o.status, 'no_order') AS status
FROM orders AS o
FULL OUTER JOIN customers AS c
    ON o.customer_id = c.customer_id
ORDER BY customer_id;
-- Result: 6 rows — order 5 (ghost customer 199) + Diana (no orders) both appear

-- -----------------------------------------------------------------------
-- 5. Anti-join pattern — customers who have NEVER placed an order
-- -----------------------------------------------------------------------

-- LEFT JOIN + WHERE right side key IS NULL is equivalent to NOT IN / NOT EXISTS
-- and is typically the most efficient form in Spark.
SELECT
    c.customer_id,
    c.name,
    c.country
FROM customers AS c
LEFT OUTER JOIN orders AS o
    ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;
-- Result: Diana (customer_id 104) — no orders on record

-- -----------------------------------------------------------------------
-- 6. Anti-join equivalent using NOT EXISTS subquery
-- -----------------------------------------------------------------------

SELECT
    c.customer_id,
    c.name,
    c.country
FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
);
-- Result: same as example 5 — Diana (customer_id 104)

-- -----------------------------------------------------------------------
-- 7. LEFT JOIN aggregation — order count per customer
--    (including customers with zero orders)
-- -----------------------------------------------------------------------

SELECT
    c.customer_id,
    c.name AS customer_name,
    COUNT(o.order_id) AS order_count,  -- 0 for customers with no orders
    COALESCE(SUM(o.amount), 0.00) AS total_spend
FROM customers AS c
LEFT OUTER JOIN orders AS o
    ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.name
ORDER BY total_spend DESC;
-- Result: Diana appears with order_count=0 and total_spend=0.00
