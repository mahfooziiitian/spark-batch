-- Operator examples in Spark SQL (Databricks).
-- Covers arithmetic, string, bitwise operators, and set operators
-- (UNION, UNION ALL, INTERSECT, EXCEPT / MINUS).

-- ----------------------------------------------------------------------------
-- Setup: products and clearance tables for set operator examples
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW all_products AS
SELECT
    product_id,
    product_name,
    category
FROM
    VALUES
    (1, 'Laptop', 'Electronics'),
    (2, 'Desk', 'Furniture'),
    (3, 'Monitor', 'Electronics'),
    (4, 'Chair', 'Furniture'),
    (5, 'Headphones', 'Electronics')
        AS t (product_id, product_name, category);

CREATE OR REPLACE TEMP VIEW clearance_products AS
SELECT
    product_id,
    product_name,
    category
FROM
    VALUES
    (3, 'Monitor', 'Electronics'),
    (4, 'Chair', 'Furniture'),
    (6, 'Webcam', 'Electronics')
        AS t (product_id, product_name, category);

-- ----------------------------------------------------------------------------
-- 1. Arithmetic operators: +, -, *, /, % (modulo), DIV (integer division)
-- ----------------------------------------------------------------------------
SELECT
    10 + 3 AS addition,         -- Result: 13
    10 - 3 AS subtraction,      -- Result: 7
    10 * 3 AS multiplication,   -- Result: 30
    10 / 3 AS division,         -- Result: 3.3333... (floating-point)
    10 % 3 AS modulo,           -- Result: 1 (remainder)
    10 DIV 3 AS int_division;   -- Result: 3 (truncates fractional part)

-- Applied to a table: compute discounted price and tax-inclusive price.
SELECT
    product_id,
    product_name,
    100.0 AS list_price,
    100.0 * 0.9 AS discounted_price,       -- 10 % discount
    100.0 * 1.2 AS price_with_tax,         -- 20 % VAT
    ROUND(100.0 / 3, 2) AS split_3_ways,   -- cost split among 3 people
    100 % 7 AS leftover_units              -- units that don't fill a shelf of 7
FROM all_products
ORDER BY product_id ASC
LIMIT 3;

-- ----------------------------------------------------------------------------
-- 2. String operators: || concatenation, CONCAT, LIKE
-- ----------------------------------------------------------------------------
SELECT
    'Hello' || ', ' || 'World!' AS pipe_concat,      -- Result: Hello, World!
    CONCAT('Hello', ', ', 'World!') AS fn_concat,    -- Result: Hello, World!
    CONCAT_WS('-', '2024', '01', '15') AS iso_date;  -- Result: 2024-01-15

SELECT
    product_id,
    product_name || ' (' || category || ')' AS labelled_name
FROM all_products;
-- Result: "Laptop (Electronics)", "Desk (Furniture)", etc.

-- LIKE as a string operator (see also condition/pattern.sql for full coverage):
SELECT product_name FROM all_products
WHERE product_name LIKE '%o%';
-- Result: Monitor, Headphones

-- ----------------------------------------------------------------------------
-- 3. Bitwise operators: &, |, ^, ~, <<, >>
-- ----------------------------------------------------------------------------
SELECT
    12 & 10 AS bitwise_and,    -- 1100 & 1010 = 1000 → Result: 8
    12 | 10 AS bitwise_or,     -- 1100 | 1010 = 1110 → Result: 14
    12 ^ 10 AS bitwise_xor,    -- 1100 ^ 1010 = 0110 → Result: 6
    -- ~0101 = ...11111010 → Result: -6 (two's complement)
    ~5 AS bitwise_not,
    1 << 3 AS left_shift,      -- 0001 << 3 = 1000   → Result: 8
    16 >> 2 AS right_shift;    -- 10000 >> 2 = 100   → Result: 4

-- Practical use: bitmask permissions (READ=1, WRITE=2, EXEC=4).
SELECT
    user_id,
    permissions,
    (permissions & 1) > 0 AS can_read,    -- bit 0 set
    (permissions & 2) > 0 AS can_write,   -- bit 1 set
    (permissions & 4) > 0 AS can_exec     -- bit 2 set
FROM
    VALUES (1, 7), (2, 5), (3, 2)
        AS t (user_id, permissions);
-- user 1 (111=7): all true; user 2 (101=5): read+exec; user 3 (010=2): write

-- ----------------------------------------------------------------------------
-- 4. UNION ALL: combine rows including duplicates
-- ----------------------------------------------------------------------------
-- Both queries must have the same number of columns with compatible types.
SELECT
    product_id,
    product_name,
    category
FROM all_products
UNION ALL
SELECT
    product_id,
    product_name,
    category
FROM clearance_products;
-- Result: 8 rows — 5 from all_products + 3 from clearance_products (dupes kept)

-- ----------------------------------------------------------------------------
-- 5. UNION (DISTINCT): combine rows and deduplicate
-- ----------------------------------------------------------------------------
SELECT
    product_id,
    product_name,
    category
FROM all_products
UNION
SELECT
    product_id,
    product_name,
    category
FROM clearance_products;
-- Result: 6 distinct rows — Monitor (3) and Chair (4) appear only once.

-- ----------------------------------------------------------------------------
-- 6. INTERSECT DISTINCT: rows present in BOTH result sets (default)
-- ----------------------------------------------------------------------------
SELECT
    product_id,
    product_name,
    category
FROM all_products
INTERSECT DISTINCT
SELECT
    product_id,
    product_name,
    category
FROM clearance_products;
-- Result: Monitor (3) and Chair (4) — in both tables.

-- INTERSECT ALL: preserves duplicates (returns a row N times if it appears
-- in both sets with min(count_left, count_right) frequency).
SELECT
    product_id,
    product_name,
    category
FROM all_products
INTERSECT ALL
SELECT
    product_id,
    product_name,
    category
FROM clearance_products;
-- Result: Monitor (3) and Chair (4) — same since each appears once per set.

-- ----------------------------------------------------------------------------
-- 7. EXCEPT (MINUS): rows in the left set that are NOT in the right set
-- ----------------------------------------------------------------------------
-- EXCEPT / MINUS are synonyms in Spark SQL.
SELECT
    product_id,
    product_name,
    category
FROM all_products
EXCEPT
SELECT
    product_id,
    product_name,
    category
FROM clearance_products;
-- Result: Laptop (1), Desk (2), Headphones (5) — in all_products not clearance.

-- MINUS (identical behaviour):
SELECT
    product_id,
    product_name,
    category
FROM all_products
MINUS
SELECT
    product_id,
    product_name,
    category
FROM clearance_products;
-- Result: same 3 rows — Laptop, Desk, Headphones.

-- Items in clearance NOT in all_products (new arrivals only in clearance):
SELECT
    product_id,
    product_name,
    category
FROM clearance_products
EXCEPT
SELECT
    product_id,
    product_name,
    category
FROM all_products;
-- Result: Webcam (6) — only in clearance_products.
