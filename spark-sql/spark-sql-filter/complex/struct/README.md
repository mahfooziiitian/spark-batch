# Struct

```sql


CREATE OR REPLACE TEMP VIEW complex_sales AS
SELECT * FROM VALUES
  ('Alice', STRUCT('NY' AS city, 'USA' AS country), ARRAY('gift', 'priority'), MAP('product', 'A', 'price', '100')),
  ('Bob', STRUCT('LA' AS city, 'USA' AS country), ARRAY('discount'), MAP('product', 'B', 'price', '200')),
  ('Charlie', STRUCT('Toronto' AS city, 'Canada' AS country), ARRAY('gift', 'exclusive'), MAP('product', 'C', 'price', '300')),
  ('Dana', STRUCT('Vancouver' AS city, 'Canada' AS country), ARRAY('promo'), MAP('product', 'A', 'price', '250'))
AS complex_sales(name, location, tags, info);

-- Get all customers in USA
SELECT * FROM complex_sales
WHERE location.country = 'USA';

-- Customers from NY only
SELECT * FROM complex_sales
WHERE location.city = 'NY';
```
