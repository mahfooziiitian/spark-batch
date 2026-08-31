-- Using posexplode() (doesn't return rows for empty/null)

CREATE OR REPLACE TEMP VIEW demo AS
SELECT--noqa
    * --noqa
FROM
    VALUES
    (1, ARRAY('A', 'B')),
    (2, ARRAY()),          -- empty array
    (3, NULL)              -- NULL array
        AS demo (id, arr);
SELECT
    id,
    pos,
    val
FROM demo
    LATERAL VIEW POSEXPLODE(arr) AS pos, val;

-- 🔍 Using posexplode_outer()


CREATE OR REPLACE TEMP VIEW demo AS
SELECT--noqa
    * --noqa
FROM
    VALUES
    (1, ARRAY('A', 'B')),
    (2, ARRAY()),          -- empty array
    (3, NULL)              -- NULL array
        AS demo (id, arr);
SELECT
    id,
    pos,
    val
FROM demo
    LATERAL VIEW POSEXPLODE_OUTER(arr) AS pos, val;

-- With Array of Structs

CREATE OR REPLACE TEMP VIEW orders AS
SELECT--noqa
    * --noqa
FROM
    VALUES
    (101, ARRAY(NAMED_STRUCT('item', 'book', 'qty', 2))),
    (102, ARRAY()),            -- Empty array
    (103, NULL)                -- NULL array
        AS orders (order_id, products);
SELECT
    orders.order_id,
    orders.pos,
    p.item,
    p.qty
FROM orders
    LATERAL VIEW POSEXPLODE_OUTER(products) AS pos, p;

-- 3. Labeling Array Entries Safely

CREATE OR REPLACE TEMP VIEW students AS
SELECT--noqa
    * --noqa
FROM
    VALUES
    (1, ARRAY('Math', 'Physics')),
    (2, NULL)
        AS students (id, subjects);
SELECT
    id,
    subject,
    CONCAT('Subject_', pos + 1) AS label
FROM students
    LATERAL VIEW POSEXPLODE_OUTER(subjects) AS pos, subject;
