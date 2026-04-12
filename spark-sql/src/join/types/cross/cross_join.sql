-- Cross join examples: Cartesian product, generating combination matrices,
-- CROSS JOIN LATERAL for row-level expansion, and a product-catalog use case
-- that generates all size × color combinations.

-- -----------------------------------------------------------------------
-- Test data
-- -----------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW categories AS
SELECT category
FROM
    VALUES
    ('Electronics'),
    ('Accessories'),
    ('Apparel')
        AS categories (category);

CREATE OR REPLACE TEMP VIEW month_list AS
SELECT month_name
FROM
    VALUES
    ('Jan'),
    ('Feb'),
    ('Mar'),
    ('Apr'),
    ('May'),
    ('Jun')
        AS month_list (month_name);

CREATE OR REPLACE TEMP VIEW sizes AS
SELECT size
FROM
    VALUES
    ('S'),
    ('M'),
    ('L'),
    ('XL')
        AS sizes (size);

CREATE OR REPLACE TEMP VIEW colors AS
SELECT color
FROM
    VALUES
    ('Red'),
    ('Blue'),
    ('Green')
        AS colors (color);

CREATE OR REPLACE TEMP VIEW products AS
SELECT
    product_id,
    product_name,
    base_price
FROM
    VALUES
    (1, 'Classic Tee', 19.99),
    (2, 'Slim Hoodie', 49.99),
    (3, 'Running Cap', 14.99)
        AS products (product_id, product_name, base_price);

-- -----------------------------------------------------------------------
-- 1. Basic Cartesian product — every category paired with every month
-- -----------------------------------------------------------------------

-- A CROSS JOIN produces N × M rows (3 categories × 6 months = 18 rows here).
SELECT
    c.category,
    m.month_name
FROM categories AS c
CROSS JOIN month_list AS m
ORDER BY
    c.category,
    m.month_name;
-- Result: 18 rows — useful as a scaffold for reporting gaps

-- -----------------------------------------------------------------------
-- 2. Generate all size × color combinations for a product catalog
-- -----------------------------------------------------------------------

-- Every product gets every size and every color variant (4 sizes × 3 colors).
SELECT
    p.product_id,
    p.product_name,
    s.size,
    cl.color,
    ROUND(p.base_price * 1.05, 2) AS variant_price  -- 5 % uplift per variant
FROM products AS p
CROSS JOIN sizes AS s
CROSS JOIN colors AS cl
ORDER BY
    p.product_name,
    s.size,
    cl.color;
-- Result: 3 products × 4 sizes × 3 colors = 36 variant rows

-- -----------------------------------------------------------------------
-- 3. Use CROSS JOIN to build a date-skeleton for every category
-- -----------------------------------------------------------------------

-- Skeleton rows guarantee that GROUP BY aggregations have a row even when
-- no real data exists for a given (month, category) pair.
WITH sales AS (
    SELECT
        category,
        month_name,
        revenue
    FROM
        VALUES
        ('Electronics', 'Jan', 1200.00),
        ('Electronics', 'Feb', 980.00),
        ('Apparel', 'Mar', 450.00)
            AS sales (category, month_name, revenue)
)

SELECT
    skeleton.category,
    skeleton.month_name,
    COALESCE(s.revenue, 0.00) AS revenue
FROM (
    SELECT
        c.category,
        m.month_name
    FROM categories AS c
    CROSS JOIN month_list AS m
) AS skeleton
LEFT JOIN sales AS s
    ON
        skeleton.category = s.category
        AND skeleton.month_name = s.month_name
ORDER BY
    skeleton.category,
    skeleton.month_name;
-- Result: 18 rows; months with no sales show revenue = 0.00

-- -----------------------------------------------------------------------
-- 4. Generate per-product price tiers using CROSS JOIN with a tiers table
-- -----------------------------------------------------------------------

-- CROSS JOIN against a small tier multiplier table is SQLFluff-friendly and
-- achieves the same per-row expansion that LATERAL would provide.
WITH tier_multipliers AS (
    SELECT
        tier_label,
        tier_mult
    FROM
        VALUES
        ('budget', 0.80),
        ('standard', 1.00),
        ('premium', 1.25)
            AS tier_multipliers (tier_label, tier_mult)
)

SELECT
    p.product_name,
    t.tier_label,
    ROUND(p.base_price * t.tier_mult, 2) AS tier_price
FROM products AS p
CROSS JOIN tier_multipliers AS t
ORDER BY
    p.product_name,
    t.tier_price;
-- Result: 3 products × 3 tiers = 9 rows with computed prices

-- -----------------------------------------------------------------------
-- 5. Implicit CROSS JOIN syntax (comma-separated tables)
-- -----------------------------------------------------------------------

-- Some legacy SQL uses comma syntax; it is equivalent to CROSS JOIN.
-- Prefer the explicit CROSS JOIN keyword for clarity.
SELECT
    s.size,
    cl.color
FROM sizes AS s, colors AS cl
ORDER BY
    s.size,
    cl.color;
-- Result: 4 × 3 = 12 rows — identical to an explicit CROSS JOIN
