# Array filter

Filter Rows with Specific Array Elements.
Returns rows that have 'gift' in the tags array.

```sql
CREATE OR REPLACE TEMP VIEW complex_sales AS
SELECT * FROM VALUES
  ('Alice', STRUCT('NY' AS city, 'USA' AS country), ARRAY('gift', 'priority'), MAP('product', 'A', 'price', '100')),
  ('Bob', STRUCT('LA' AS city, 'USA' AS country), ARRAY('discount'), MAP('product', 'B', 'price', '200')),
  ('Charlie', STRUCT('Toronto' AS city, 'Canada' AS country), ARRAY('gift', 'exclusive'), MAP('product', 'C', 'price', '300')),
  ('Dana', STRUCT('Vancouver' AS city, 'Canada' AS country), ARRAY('promo'), MAP('product', 'A', 'price', '250'))
AS complex_sales(name, location, tags, info);
```

## Check if a value exists in an array

```sql
-- Customers tagged as "gift"
SELECT * FROM complex_sales
WHERE array_contains(tags, 'gift');
```

## Match arrays using size() or element_at()

```sql
-- Customers with more than 1 tag
SELECT * FROM complex_sales
WHERE size(tags) > 1;

-- Customers whose first tag is 'gift'
SELECT * FROM complex_sales
WHERE element_at(tags, 1) = 'gift';
```
