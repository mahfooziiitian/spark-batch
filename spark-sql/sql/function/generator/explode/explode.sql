--  1. Exploding an Array of Primitives

CREATE OR REPLACE TEMP VIEW people AS
SELECT -- noqa: LT09
    * FROM VALUES
(1, ARRAY('Alice', 'Bob')),
(2, ARRAY('Charlie', 'Diana'))
    AS people (id, names);
SELECT id, name
FROM people
    LATERAL VIEW EXPLODE(names) AS name;

-- 2. Exploding a Map

CREATE OR REPLACE TEMP VIEW sales AS
SELECT
    * FROM VALUES
(1, MAP('apple', 2, 'banana', 3)),
(2, MAP('orange', 1, 'grape', 5))
    AS sales (id, items);
SELECT id, fruit, quantity
FROM sales
    LATERAL VIEW EXPLODE(items) AS fruit, quantity;

-- 3. Exploding an Array of Structs

CREATE OR REPLACE TEMP VIEW orders AS
SELECT
    * FROM VALUES
(1001, ARRAY(
    NAMED_STRUCT('product', 'book', 'qty', 2),
    NAMED_STRUCT('product', 'pen', 'qty', 5)
)),
(1002, ARRAY(
    NAMED_STRUCT('product', 'notebook', 'qty', 1),
    NAMED_STRUCT('product', 'eraser', 'qty', 3)
))
    AS orders (order_id, products);
SELECT orders.order_id, item.product, item.qty AS quantity
FROM orders
    LATERAL VIEW EXPLODE(products) AS item;

-- 4. Explode with SEQUENCE() to Create Date Ranges

SELECT EXPLODE(SEQUENCE(DATE '2024-01-01', DATE '2024-01-05')) AS day;

-- 5. Combine with TRANSFORM and FILTER

SELECT EXPLODE(
    FILTER(ARRAY(1, 2, 3, 4, 5), x -> x % 2 = 0)
) AS even;
