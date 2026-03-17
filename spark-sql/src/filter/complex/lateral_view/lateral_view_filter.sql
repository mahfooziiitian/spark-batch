-- Lateral view filter
CREATE OR REPLACE TEMP VIEW complex_sales AS
SELECT *
FROM
    VALUES (
        'Alice',
        STRUCT('NY' AS city, 'USA' AS country),
        ARRAY('gift', 'priority'),
        MAP('product', 'A', 'price', '100')
    ),
    (
        'Bob',
        STRUCT('LA' AS city, 'USA' AS country),
        ARRAY('discount'),
        MAP('product', 'B', 'price', '200')
    ),
    (
        'Charlie',
        STRUCT('Toronto' AS city, 'Canada' AS country),
        ARRAY('gift', 'exclusive'),
        MAP('product', 'C', 'price', '300')
    ),
    (
        'Dana',
        STRUCT('Vancouver' AS city, 'Canada' AS country),
        ARRAY('promo'),
        MAP('product', 'A', 'price', '250')
    ) AS complex_sales (name, location, tags, info);
-- Explode tags and filter by each
SELECT
    name,
    tag
FROM complex_sales LATERAL VIEW EXPLODE(c.tags) AS tag
WHERE tag = 'gift';
-- Others
-- Explode map to filter price directly
SELECT
    name,
    key,
    value
FROM
    complex_sales
    LATERAL VIEW EXPLODE(info) AS key,
    value
WHERE
    key = 'price'
    AND CAST(value AS INT) > 200;
