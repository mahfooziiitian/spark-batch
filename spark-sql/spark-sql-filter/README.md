# Spark SQL filter

In Spark SQL, FILTER operations are fundamental for controlling and refining the data you're analyzing.

Filtering is powerful, but subtle features (like predicate pushdown, null handling, WHERE vs HAVING, and UDF effects) often trip people up.

## ✅ 1. Basic WHERE Filtering

```sql

select 
  * 
from 
  states_population
where 
  state='California'
```

### ➕ Operators

1. `Comparison`: =, !=, >, <, >=, <=
2. `Logical`: AND, OR, NOT
3. `Pattern`: LIKE, RLIKE, IN, BETWEEN, IS NULL, IS NOT NULL

## ✅ 2. FILTER Clause in Aggregates (Advanced)

Spark SQL supports the FILTER clause on aggregation functions to selectively aggregate.

```sql

SELECT
  COUNT(*) AS total_orders,
  COUNT(*) FILTER (WHERE status = 'shipped') AS shipped_orders
FROM orders;
```

### Multiple conditional aggregates

```sql

SELECT
  region,
  SUM(sales) FILTER (WHERE product = 'A') AS product_a_sales,
  SUM(sales) FILTER (WHERE product = 'B') AS product_b_sales
FROM transactions
GROUP BY region;
```

## ✅ 3. HAVING for Filtering After Aggregation

Use HAVING to filter after aggregation, often in combo with GROUP BY.

```sql

SELECT region, COUNT(*) AS total_orders
FROM orders
GROUP BY region
HAVING COUNT(*) > 100;
```

## ✅ 4. NULL Handling in Filters

NULLs require careful treatment.
Always use IS NULL and IS NOT NULL for null checks.

```sql

-- Will NOT match null values
SELECT * FROM users WHERE last_login != '2023-01-01';

-- To include NULLs explicitly:
SELECT * FROM users WHERE last_login != '2023-01-01' OR last_login IS NULL;

```

## ✅ 5. Subquery Filtering

```sql

SELECT * FROM orders
WHERE customer_id IN (SELECT id FROM customers WHERE country = 'US');

SELECT * FROM products p
WHERE EXISTS (
  SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.stock > 0
);
```

## ✅ 6. Filtering Structs, Arrays, Maps (Complex Types)

### Struct Example

```sql
SELECT * FROM users WHERE address.city = 'New York';
```

### Array Contains

```sql
SELECT * FROM orders WHERE array_contains(tags, 'priority');
```

### Map Filtering

```sql

SELECT * FROM kv_store WHERE my_map['key1'] = 'value1';
```
