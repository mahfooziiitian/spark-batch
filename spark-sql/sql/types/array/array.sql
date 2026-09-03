-- ARRAY type examples in Spark SQL (Databricks dialect).
-- Covers creation, indexing, set operations, aggregation, and struct arrays.

CREATE OR REPLACE TEMP VIEW orders AS
SELECT *
FROM
    VALUES
    (1, 'Alice', ARRAY(10, 20, 30), ARRAY('gift', 'priority')),
    (2, 'Bob', ARRAY(5, 5, 10), ARRAY('discount')),
    (3, 'Carol', ARRAY(7, 14, 21), ARRAY('gift', 'exclusive', 'promo')),
    (4, 'Dana', ARRAY(1, 2), ARRAY('promo'))
        AS orders (id, name, amounts, tags);

CREATE OR REPLACE TEMP VIEW products AS
SELECT *
FROM
    VALUES
    (
        1,
        ARRAY(
            STRUCT('laptop' AS item, 999.99 AS price),
            STRUCT('mouse' AS item, 29.99 AS price)
        )
    ),
    (2, ARRAY(STRUCT('keyboard' AS item, 79.99 AS price))),
    (
        3,
        ARRAY(
            STRUCT('monitor' AS item, 349.99 AS price),
            STRUCT('cable' AS item, 9.99 AS price)
        )
    )
        AS products (order_id, line_items);

---
-- 1. Array literals
---

SELECT
    ARRAY(1, 2, 3) AS int_array,    -- Result: [1, 2, 3]
    ARRAY('a', 'b', 'c') AS str_array,    -- Result: ['a', 'b', 'c']
    ARRAY(1.0, 2.5, 3.14) AS dbl_array;    -- Result: [1.0, 2.5, 3.14]

---
-- 2. Index access
---

-- 0-based bracket notation
SELECT
    name,
    amounts[0] AS first_amount,   -- Result: first element
    amounts[1] AS second_amount
FROM orders;

-- 1-based ELEMENT_AT (returns NULL for out-of-bounds)
SELECT
    name,
    ELEMENT_AT(amounts, 1) AS first_amount,   -- Result: first element
    ELEMENT_AT(tags, -1) AS last_tag        -- negative index counts from end
FROM orders;

---
-- 3. Size / cardinality
---

SELECT
    name,
    SIZE(amounts) AS num_amounts,   -- Result: 3, 3, 3, 2
    CARDINALITY(amounts) AS cardinality    -- alias for SIZE
FROM orders;

---
-- 4. ARRAY_CONTAINS
---

-- Customers tagged as 'gift'
SELECT name
FROM orders
WHERE ARRAY_CONTAINS(tags, 'gift');
-- Result: Alice, Carol

---
-- 5. Set operations
---

SELECT
    -- Result: [1, 2, 3]
    ARRAY_DISTINCT(ARRAY(1, 2, 2, 3, 3)) AS distinct_vals,
    -- Result: [1, 2, 3, 4]
    ARRAY_UNION(ARRAY(1, 2, 3), ARRAY(2, 3, 4)) AS union_vals,
    -- Result: [2, 3]
    ARRAY_INTERSECT(ARRAY(1, 2, 3), ARRAY(2, 3, 4)) AS intersect_vals,
    -- Result: [1]
    ARRAY_EXCEPT(ARRAY(1, 2, 3), ARRAY(2, 3, 4)) AS except_vals;

---
-- 6. SORT_ARRAY
---

SELECT
    name,
    SORT_ARRAY(amounts) AS asc_amounts,  -- ascending (default)
    SORT_ARRAY(amounts, FALSE) AS desc_amounts  -- descending
FROM orders;

---
-- 7. ARRAY_APPEND, ARRAY_PREPEND, ARRAY_REMOVE
---

SELECT
    name,
    ARRAY_APPEND(tags, 'new') AS appended,   -- Result: [..., 'new']
    ARRAY_PREPEND('vip', tags) AS prepended,  -- Result: ['vip', ...]
    ARRAY_REMOVE(tags, 'gift') AS removed     -- Result: tags without 'gift'
FROM orders;

---
-- 8. FLATTEN (nested arrays)
---

SELECT FLATTEN(ARRAY(ARRAY(1, 2), ARRAY(3, 4), ARRAY(5))) AS flat;
-- Result: [1, 2, 3, 4, 5]

---
-- 9. ARRAY_POSITION
---

-- 1-based position; returns 0 if not found
SELECT
    name,
    ARRAY_POSITION(tags, 'gift') AS gift_pos,      -- Result: 1 or 0
    ARRAY_POSITION(tags, 'exclusive') AS exclusive_pos
FROM orders;

---
-- 10. SLICE
---

-- SLICE(array, start, length) — 1-based start
SELECT
    name,
    SLICE(amounts, 1, 2) AS first_two,  -- Result: first 2 elements
    SLICE(amounts, 2, 2) AS mid         -- Result: elements 2–3
FROM orders;

---
-- 11. CONCAT to merge two arrays
---

SELECT
    name,
    -- Result: tags + ['loyalty']
    CONCAT(tags, ARRAY('loyalty')) AS extended_tags
FROM orders;

---
-- 12. COLLECT_LIST and COLLECT_SET aggregation
---

SELECT
    COLLECT_LIST(name) AS all_names,   -- Result: list with duplicates preserved
    COLLECT_SET(name) AS unique_names -- Result: distinct names
FROM orders;

-- Collect amounts per customer (after explode round-trip)
SELECT
    name,
    COLLECT_LIST(amount) AS amounts_list
FROM (
    SELECT
        name,
        EXPLODE(amounts) AS amount
    FROM orders
) AS exploded
GROUP BY name;

---
-- 13. ARRAYS_OVERLAP
---

SELECT ARRAYS_OVERLAP(ARRAY('gift', 'priority'), ARRAY('priority', 'promo'))
    AS has_overlap;
-- Result: true

SELECT
    name,
    ARRAYS_OVERLAP(tags, ARRAY('gift', 'exclusive')) AS has_promo_tag
FROM orders;

---
-- 14. Array of structs — access struct fields
---

SELECT
    order_id,
    line_items[0].item AS first_item,
    line_items[0].price AS first_item_price,
    SIZE(line_items) AS num_line_items
FROM products;

-- Explode array of structs into rows
SELECT
    products.order_id,
    item_row.item,
    item_row.price
FROM products
    LATERAL VIEW EXPLODE(line_items) AS item_row;

-- Total order value (sum prices across struct array)
SELECT
    order_id,
    AGGREGATE(
        line_items,
        CAST(0.0 AS DOUBLE),
        (acc, x) -> acc + x.price
    ) AS order_total
FROM products;
