-- Array filter
-- Filter Rows with Specific Array Elements.
-- Returns rows that have 'gift' in the tags array.


CREATE OR REPLACE TEMP VIEW complex_sales AS
SELECT
    * FROM VALUES
(
    'Alice',
    STRUCT('NY' as city, 'USA' as country),
    ARRAY('gift', 'priority'),
    MAP('product', 'A', 'price', '100')
),
(
    'Bob',
    STRUCT('LA' as city, 'USA' as country),
    ARRAY('discount'),
    MAP('product', 'B', 'price', '200')
),
(
    'Charlie',
    STRUCT('Toronto' as city, 'Canada' as country),
    ARRAY('gift', 'exclusive'),
    MAP('product', 'C', 'price', '300')
),
(
    'Dana',
    STRUCT('Vancouver' as city, 'Canada' as country),
    ARRAY('promo'),
    MAP('product', 'A', 'price', '250')
)
    AS complex_sales (name, location, tags, info);

-- Check if a value exists in an array

-- Customers tagged as "gift"
SELECT * FROM complex_sales
WHERE ARRAY_CONTAINS(tags, 'gift');

EXPLAIN SELECT * FROM complex_sales
WHERE ARRAY_CONTAINS(tags, 'gift');

-- Match arrays using size() or element_at()

-- Customers with more than 1 tag
SELECT * FROM complex_sales
WHERE SIZE(tags) > 1;

-- Customers whose first tag is 'gift'
SELECT * FROM complex_sales
WHERE ELEMENT_AT(tags, 1) = 'gift';
