-- Subquery examples in Spark SQL (Databricks).
-- Covers: scalar subqueries, IN / NOT IN / EXISTS / NOT EXISTS,
-- correlated subqueries, derived tables, and HAVING subqueries.

-- ----------------------------------------------------------------------------
-- Setup: customers, orders, order_items inline data
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW customers AS
SELECT
    customer_id,
    customer_name,
    country
FROM
    VALUES
    ('C1', 'Alice', 'US'),
    ('C2', 'Bob', 'US'),
    ('C3', 'Carol', 'UK'),
    ('C4', 'Dave', 'UK'),
    ('C5', 'Eve', 'US')
        AS t (customer_id, customer_name, country);

CREATE OR REPLACE TEMP VIEW orders AS
SELECT
    order_id,
    customer_id,
    amount,
    order_date
FROM
    VALUES
    (1, 'C1', 120.0, DATE '2024-01-05'),
    (2, 'C2', 45.0, DATE '2024-01-06'),
    (3, 'C1', 300.0, DATE '2024-01-07'),
    (4, 'C3', 80.0, DATE '2024-01-08'),
    (5, 'C2', 210.0, DATE '2024-01-09'),
    (6, 'C1', 95.0, DATE '2024-01-10')
        AS t (order_id, customer_id, amount, order_date);

CREATE OR REPLACE TEMP VIEW order_items AS
SELECT
    order_id,
    item_id,
    unit_price
FROM
    VALUES
    (1, 'I1', 60.0),
    (1, 'I2', 60.0),
    (2, 'I3', 45.0),
    (3, 'I4', 150.0),  -- > $100
    (3, 'I5', 150.0),  -- > $100
    (4, 'I6', 80.0),
    (5, 'I7', 110.0),  -- > $100
    (6, 'I8', 95.0)
        AS t (order_id, item_id, unit_price);

-- ----------------------------------------------------------------------------
-- 1. Scalar subquery in SELECT: inline aggregation per row
-- ----------------------------------------------------------------------------
-- Show each order alongside the overall average order amount.
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    (SELECT AVG(inner_o.amount) FROM orders AS inner_o) AS global_avg_amount
FROM orders AS o;
-- Result: every row shows its own amount plus the fleet average (~141.67).

-- ----------------------------------------------------------------------------
-- 2. Scalar subquery in WHERE: compare to overall average
-- ----------------------------------------------------------------------------
-- Orders with amount above the global average.
SELECT
    o.order_id,
    o.customer_id,
    o.amount
FROM orders AS o
WHERE o.amount > (SELECT AVG(inner_o.amount) FROM orders AS inner_o);
-- Result: orders 1 (120→ below avg 141.67), 3 (300), 5 (210).

-- ----------------------------------------------------------------------------
-- 3. IN subquery: orders for US customers only
-- ----------------------------------------------------------------------------
SELECT
    o.order_id,
    o.customer_id,
    o.amount
FROM orders AS o
WHERE o.customer_id IN (
    SELECT c.customer_id
    FROM customers AS c
    WHERE c.country = 'US'
);
-- Result: orders belonging to Alice (C1), Bob (C2), and Eve (C5, no orders).

-- ----------------------------------------------------------------------------
-- 4. NOT IN subquery
-- ----------------------------------------------------------------------------
-- ⚠ NULL trap: if the subquery returns ANY NULL, NOT IN returns no rows.
--   Always add WHERE <col> IS NOT NULL inside the subquery to be safe.
SELECT
    o.order_id,
    o.customer_id,
    o.amount
FROM orders AS o
WHERE o.customer_id NOT IN (
    SELECT c.customer_id
    FROM customers AS c
    WHERE
        c.country = 'US'
        AND c.customer_id IS NOT NULL  -- guard against NULL trap
);
-- Result: orders for UK customers Carol (C3) and Dave (C4, no orders).

-- ----------------------------------------------------------------------------
-- 5. EXISTS: orders that have at least one item > $100
-- ----------------------------------------------------------------------------
SELECT
    o.order_id,
    o.customer_id,
    o.amount
FROM orders AS o
WHERE EXISTS (
    SELECT 1
    FROM order_items AS i
    WHERE
        i.order_id = o.order_id
        AND i.unit_price > 100.0
);
-- Result: orders 3 (150+150) and 5 (110).

-- ----------------------------------------------------------------------------
-- 6. NOT EXISTS: customers with no orders
-- ----------------------------------------------------------------------------
SELECT
    c.customer_id,
    c.customer_name
FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
);
-- Result: Eve (C5) and Dave (C4) have no orders.

-- ----------------------------------------------------------------------------
-- 7. Correlated subquery: orders above each customer's own average
-- ----------------------------------------------------------------------------
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    (
        SELECT AVG(o2.amount)
        FROM orders AS o2
        WHERE o2.customer_id = o.customer_id
    ) AS customer_avg
FROM orders AS o
WHERE o.amount > (
    SELECT AVG(o3.amount)
    FROM orders AS o3
    WHERE o3.customer_id = o.customer_id
);
-- Result: Alice order 3 (300 > avg 171.67), Bob order 5 (210 > avg 127.5).

-- ----------------------------------------------------------------------------
-- 8. Derived table (subquery in FROM) with aggregation
-- ----------------------------------------------------------------------------
-- Aggregate once in the subquery, then filter on the derived result.
SELECT
    dt.customer_id,
    dt.total_amount
FROM (
    SELECT
        ord.customer_id,
        SUM(ord.amount) AS total_amount
    FROM orders AS ord
    GROUP BY ord.customer_id
) AS dt
WHERE dt.total_amount > 200.0;
-- Result: Alice (C1) 515.0, Bob (C2) 255.0.

-- ----------------------------------------------------------------------------
-- 9. Subquery in HAVING clause
-- ----------------------------------------------------------------------------
-- Customers whose total spend exceeds the average total spend per customer.
SELECT
    o.customer_id,
    SUM(o.amount) AS total_amount
FROM orders AS o
GROUP BY o.customer_id
HAVING SUM(o.amount) > (
    SELECT AVG(cs.cust_total)
    FROM (
        SELECT
            customer_id,
            SUM(amount) AS cust_total
        FROM orders
        GROUP BY customer_id
    ) AS cs
);
-- Result: Alice (C1) 515.0 exceeds the per-customer average.
