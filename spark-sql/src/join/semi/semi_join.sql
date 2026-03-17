-- Semi-join and anti-join examples using LEFT SEMI JOIN and LEFT ANTI JOIN.
-- Semi join: existence check — returns only left-side columns
-- when a match exists.
-- Anti join: non-existence check — returns only left-side rows with NO match.
-- Equivalence to EXISTS / NOT EXISTS subqueries is shown for each.
-- Tables: orders(order_id, customer_id, amount, status),
--         customers(customer_id, name, country),
--         vip_customers(customer_id)

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
    (5, 199, 960.00, 'cancelled')
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

-- VIP tier: only Alice and Charlie
CREATE OR REPLACE TEMP VIEW vip_customers AS
SELECT customer_id
FROM
    VALUES
    (101),
    (103)
        AS vip_customers (customer_id);

-- -----------------------------------------------------------------------
-- 1. LEFT SEMI JOIN — customers who have at least one order
-- -----------------------------------------------------------------------

-- Only left-side columns are returned; no right-side columns are projected.
-- Duplicate left rows are NOT produced even when multiple right rows match.
SELECT
    c.customer_id,
    c.name,
    c.country
FROM customers AS c
LEFT SEMI JOIN orders AS o  -- noqa: ST11, AL05
    ON c.customer_id = o.customer_id;
-- Result: Alice, Bob, Charlie — Diana has no orders and is excluded
-- Note: order_id 5 (customer 199) is ignored — it has no left-side match

-- -----------------------------------------------------------------------
-- 2. LEFT SEMI JOIN equivalent using EXISTS subquery
-- -----------------------------------------------------------------------

SELECT
    c.customer_id,
    c.name,
    c.country
FROM customers AS c
WHERE EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
);
-- Result: same as example 1

-- -----------------------------------------------------------------------
-- 3. LEFT SEMI JOIN — customers who are VIPs AND have a completed order
-- -----------------------------------------------------------------------

-- Filter the right side within the join condition to narrow down matches.
SELECT
    c.customer_id,
    c.name,
    c.country
FROM customers AS c
LEFT SEMI JOIN orders AS o  -- noqa: ST11, AL05
    ON
        c.customer_id = o.customer_id
        AND o.status = 'completed'
LEFT SEMI JOIN vip_customers AS v  -- noqa: ST11, AL05
    ON c.customer_id = v.customer_id;
-- Result: Alice (101), Charlie (103) — both are VIPs with a completed order

-- -----------------------------------------------------------------------
-- 4. LEFT ANTI JOIN — customers who have NO orders (orphan detection)
-- -----------------------------------------------------------------------

-- Returns only rows from the left side that do NOT have any matching right row.
SELECT
    c.customer_id,
    c.name,
    c.country
FROM customers AS c
LEFT ANTI JOIN orders AS o  -- noqa: ST11, AL05
    ON c.customer_id = o.customer_id;
-- Result: Diana (104) — the only customer with no orders
-- -----------------------------------------------------------------------
-- 5. LEFT ANTI JOIN equivalent using NOT EXISTS subquery
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
-- Result: same as example 4 — Diana (104)

-- -----------------------------------------------------------------------
-- 6. LEFT ANTI JOIN — orders referencing non-existent customers (orphan orders)
-- -----------------------------------------------------------------------

SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    o.status
FROM orders AS o
LEFT ANTI JOIN customers AS c  -- noqa: ST11, AL05
    ON o.customer_id = c.customer_id;
-- Result: order_id 5 (customer_id 199) — no matching customer row

-- -----------------------------------------------------------------------
-- 7. LEFT ANTI JOIN — customers who are NOT VIPs
-- -----------------------------------------------------------------------

SELECT
    c.customer_id,
    c.name,
    c.country
FROM customers AS c
LEFT ANTI JOIN vip_customers AS v  -- noqa: ST11, AL05
    ON c.customer_id = v.customer_id;
-- Result: Bob (102), Diana (104) — neither is in the VIP list
