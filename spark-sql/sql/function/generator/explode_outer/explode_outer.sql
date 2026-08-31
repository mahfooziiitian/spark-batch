-- 1. Basic Difference: EXPLODE() vs EXPLODE_OUTER()

-- Using EXPLODE() (drops NULL/empty)

CREATE OR REPLACE TEMP VIEW sample AS
SELECT
    * --noqa
FROM
    VALUES
    (1, ARRAY('A', 'B')),
    (2, ARRAY()),
    (3, NULL)
        AS sample (id, arr);
SELECT id, val
FROM sample
    LATERAL VIEW EXPLODE(arr) AS val;

--  Using EXPLODE_OUTER()


CREATE OR REPLACE TEMP VIEW sample AS
SELECT
    * FROM VALUES
(1, ARRAY('A', 'B')),
(2, ARRAY()),
(3, NULL)
    AS sample (id, arr);
SELECT id, val
FROM sample
    LATERAL VIEW EXPLODE_OUTER(arr) AS val;


-- Exploding Array of Structs Safely

CREATE OR REPLACE TEMP VIEW orders AS
SELECT
    * FROM VALUES
(1001, ARRAY(NAMED_STRUCT('product', 'pen', 'qty', 3))),
(1002, ARRAY()),
(1003, NULL)
    AS orders (order_id, items);
SELECT orders.order_id, item.product, item.qty
FROM orders
    LATERAL VIEW EXPLODE_OUTER(items) AS item;

-- Exploding a Map with EXPLODE_OUTER

CREATE OR REPLACE TEMP VIEW inventory AS
SELECT
    * FROM VALUES
(1, MAP('apple', 10, 'banana', 5)),
(2, MAP()),
(3, NULL)
    AS inventory (id, stock);
SELECT id, fruit, quantity
FROM inventory
    LATERAL VIEW EXPLODE_OUTER(stock) AS fruit, quantity;

-- 4. Exploding with SEQUENCE() and Fallback

CREATE OR REPLACE TEMP VIEW ranges AS
SELECT
    * FROM VALUES
(1, SEQUENCE(1, 3)),
(2, ARRAY()),
(3, NULL)
    AS ranges (id, numbers);
SELECT id, num
FROM ranges
    LATERAL VIEW EXPLODE_OUTER(numbers) AS num;

-- Bonus Tip: Use with IF, COALESCE

CREATE OR REPLACE TEMP VIEW sample AS
SELECT
    * FROM VALUES
(1, ARRAY('A', 'B')),
(2, ARRAY()),
(3, NULL)
    AS sample (id, arr);
SELECT
    id,
    COALESCE(val, 'no data') AS safe_val
FROM sample
    LATERAL VIEW EXPLODE_OUTER(arr) AS val;
