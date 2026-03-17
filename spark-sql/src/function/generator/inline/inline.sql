-- 1. Flatten Array of Structs into Multiple Rows & Columns

CREATE OR REPLACE TEMP VIEW orders AS
SELECT
    * FROM VALUES
(1, ARRAY(
    NAMED_STRUCT('product', 'book', 'qty', 2),
    NAMED_STRUCT('product', 'pen', 'qty', 5)
))
    AS orders (order_id, items);
SELECT order_id, product, qty
FROM orders
    LATERAL VIEW INLINE(items) AS product, qty;

-- 2. inline_outer() – Keep NULL if array is empty

CREATE OR REPLACE TEMP VIEW orders_null AS
SELECT
    * FROM VALUES
(1, ARRAY()),
(2, ARRAY(NAMED_STRUCT('product', 'pen', 'qty', 5)))
    AS orders (order_id, items);
SELECT order_id, product, qty
FROM orders_null
    LATERAL VIEW INLINE_OUTER(items) AS product, qty;
