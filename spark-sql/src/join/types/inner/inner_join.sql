-- Inner join examples: basic equi-join, multi-column join,
-- join with filter pushed down, join with aggregation,
-- self-join, multiple table join (3 tables).
-- Tables: orders(order_id, customer_id, amount, status),
--         customers(customer_id, name, country),
--         products(product_id, name, category),
--         order_items(order_id, product_id, qty)

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
    (5, 104, 960.00, 'cancelled'),
    (6, 102, 310.00, 'pending')
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
    (104, 'Diana', 'UK')
        AS customers (customer_id, name, country);

CREATE OR REPLACE TEMP VIEW products AS
SELECT
    product_id,
    name,
    category
FROM
    VALUES
    (10, 'Widget A', 'Electronics'),
    (11, 'Widget B', 'Electronics'),
    (12, 'Gadget X', 'Accessories'),
    (13, 'Gadget Y', 'Accessories')
        AS products (product_id, name, category);

CREATE OR REPLACE TEMP VIEW order_items AS
SELECT
    order_id,
    product_id,
    qty
FROM
    VALUES
    (1, 10, 2),
    (1, 12, 1),
    (2, 11, 3),
    (3, 10, 1),
    (4, 13, 5),
    (5, 11, 2),
    (6, 12, 4)
        AS order_items (order_id, product_id, qty);

-- -----------------------------------------------------------------------
-- 1. Basic equi-join — match every order to its customer
-- -----------------------------------------------------------------------

-- Returns only rows where customer_id exists in both tables.
SELECT
    o.order_id,
    c.name AS customer_name,
    c.country,
    o.amount,
    o.status
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id;
-- Result: 6 rows, one per order — all orders have matching customers here

-- -----------------------------------------------------------------------
-- 2. Join with filter pushed down — completed orders for US customers
-- -----------------------------------------------------------------------

-- Filtering before the join (via WHERE) lets the optimizer push predicates
-- into each scan, reducing data shuffled across the network.
SELECT
    o.order_id,
    c.name AS customer_name,
    o.amount
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id
WHERE
    o.status = 'completed'
    AND c.country = 'US';
-- Result: order_id 1 (Alice / 250.00), order_id 4 (Charlie / 120.00)

-- -----------------------------------------------------------------------
-- 3. Join with aggregation — total spend per customer
-- -----------------------------------------------------------------------

SELECT
    c.name AS customer_name,
    c.country,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount) AS total_spend,
    ROUND(AVG(o.amount), 2) AS avg_order_value
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id
GROUP BY
    c.name,
    c.country
ORDER BY total_spend DESC;
-- Result:
--   Diana   | UK | 1 | 960.00 | 960.00
--   Alice   | US | 2 | 680.00 | 340.00
--   Bob     | CA | 2 | 390.00 | 195.00
--   Charlie | US | 1 | 120.00 | 120.00

-- -----------------------------------------------------------------------
-- 4. Multi-column join — join order_items to both orders and products
-- -----------------------------------------------------------------------

SELECT
    oi.order_id,
    p.name AS product_name,
    p.category,
    oi.qty,
    ROUND(oi.qty * o.amount / 100, 2) AS line_value  -- illustrative
FROM order_items AS oi
INNER JOIN orders AS o
    ON oi.order_id = o.order_id
INNER JOIN products AS p
    ON oi.product_id = p.product_id
ORDER BY
    oi.order_id,
    p.name;

-- -----------------------------------------------------------------------
-- 5. Three-table join — full order detail with customer and product names
-- -----------------------------------------------------------------------

SELECT
    o.order_id,
    c.name AS customer_name,
    p.name AS product_name,
    p.category,
    oi.qty,
    o.amount,
    o.status
FROM orders AS o
INNER JOIN customers AS c
    ON o.customer_id = c.customer_id
INNER JOIN order_items AS oi
    ON o.order_id = oi.order_id
INNER JOIN products AS p
    ON oi.product_id = p.product_id
ORDER BY
    o.order_id,
    p.name;

-- -----------------------------------------------------------------------
-- 6. Self-join — find customers who share the same country
-- -----------------------------------------------------------------------

-- Pairs every customer with all other customers in the same country.
-- c1.customer_id < c2.customer_id avoids duplicate pairs.
SELECT
    c1.name AS customer_a,
    c2.name AS customer_b,
    c1.country
FROM customers AS c1
INNER JOIN customers AS c2
    ON
        c1.country = c2.country
        AND c1.customer_id < c2.customer_id;
-- Result: (Alice, Charlie, US) — only US has more than one customer
