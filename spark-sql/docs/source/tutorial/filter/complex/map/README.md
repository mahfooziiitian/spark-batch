# Map filtering

```sql

CREATE OR REPLACE TEMP VIEW complex_sales AS
SELECT * FROM VALUES
  ('Alice', STRUCT('NY' AS city, 'USA' AS country), ARRAY('gift', 'priority'), MAP('product', 'A', 'price', '100')),
  ('Bob', STRUCT('LA', 'USA'), ARRAY('discount'), MAP('product', 'B', 'price', '200')),
  ('Charlie', STRUCT('Toronto', 'Canada'), ARRAY('gift', 'exclusive'), MAP('product', 'C', 'price', '300')),
  ('Dana', STRUCT('Vancouver', 'Canada'), ARRAY('promo'), MAP('product', 'A', 'price', '250'))
AS complex_sales(name, location, tags, info);
-- Get all customers in USA
SELECT * FROM complex_sales
WHERE location.country = 'USA';
```

## Filter by key's value

```sql
-- Customers whose product is 'A'
SELECT * FROM complex_sales
WHERE info['product'] = 'A';

-- Customers where price > 200
SELECT * FROM complex_sales
WHERE CAST(info['price'] AS INT) > 200;
```

## Check if key exists in map

```sql
-- Only if key contains 'discount'
SELECT * FROM complex_sales
WHERE array_join(map_keys(info), '') LIKE '%discount%';
```

```sql
-- Only if 'discount' is a key
SELECT * FROM complex_sales
WHERE array_contains(map_keys(info), 'discount');
```

```sql

SELECT * FROM complex_sales
WHERE location.country = 'Canada'
  AND info['product'] = 'A'
  AND array_contains(tags, 'promo');
```
