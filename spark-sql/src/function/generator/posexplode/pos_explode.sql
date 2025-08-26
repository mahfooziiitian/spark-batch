-- Explode an Array with Index
CREATE OR REPLACE TEMP VIEW people AS
SELECT *
FROM
    VALUES (1, ARRAY('Alice', 'Bob', 'Charlie')),
    (2, ARRAY('Diana', 'Eve')) AS people (id, names);
SELECT
    id,
    pos,
    name
FROM people
    LATERAL VIEW POSEXPLODE(names) as pos,
    name;
-- With Array of Structs
CREATE OR REPLACE TEMP VIEW orders AS
SELECT *
FROM
    VALUES (
        1001,
        ARRAY(
            NAMED_STRUCT('product', 'book', 'qty', 2),
            NAMED_STRUCT('product', 'pen', 'qty', 5)
        )
    ) AS orders (order_id, products);
SELECT
    orders.order_id,
    orders.pos,
    item.product AS product,
    item.qty AS qty
FROM orders
    LATERAL VIEW POSEXPLODE(products) as pos,
    item;
-- Label Choices with Index
CREATE OR REPLACE TEMP VIEW survey AS
SELECT *
FROM
    VALUES (101, ARRAY('A', 'B', 'C')) AS survey (user_id, answers);
SELECT
    user_id,
    answer,
    CONCAT('Q', pos + 1) AS question
FROM survey
    LATERAL VIEW POSEXPLODE(answers) as pos,
    answer;

-- Use with sequence()
WITH sequence_ds AS (
    SELECT SEQUENCE(1, 5) AS seq
)

SELECT DATE_ADD('2024-01-01', pos) AS date
FROM (
    SELECT POSEXPLODE(seq) AS (pos, val)
    FROM sequence_ds
) AS t;

-- Compare with explode()
WITH array_ds AS (
    SELECT ARRAY('x', 'y') AS vals
)

SELECT val
FROM array_ds
    LATERAL VIEW EXPLODE(vals) as val;

WITH array_ds AS (
    SELECT ARRAY('x', 'y') AS vals
)

SELECT
    pos,
    val
FROM
    array_ds
        LATERAL VIEW POSEXPLODE(vals) as pos,
        val;
