-- Map filtering
drop view if exists complex_sales;

CREATE OR REPLACE TEMP VIEW complex_sales AS
SELECT *
FROM
VALUES (
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
  ) AS complex_sales(name, location, tags, info);

-- Get all customers in USA
SELECT *
FROM complex_sales
WHERE location.country = 'USA';
-- Filter by key's value
-- Customers whose product is 'A'
SELECT *
FROM complex_sales
WHERE info ['product'] = 'A';
-- Customers where price > 200
SELECT *
FROM complex_sales
WHERE CAST(info ['price'] AS INT) > 200;
-- Check if key exists in map
-- Only if key contains 'discount'
SELECT *
FROM complex_sales
WHERE array_join(map_keys(info), '') LIKE '%discount%';
-- Only if 'discount' is a key
SELECT *
FROM complex_sales
WHERE array_contains(map_keys(info), 'discount');
SELECT *
FROM complex_sales
WHERE location.country = 'Canada'
  AND info ['product'] = 'A'
  AND array_contains(tags, 'promo');