# Inline generator function

In Spark SQL, inline generator functions are special functions that return multiple rows or columns from a single value, often used to explode or flatten complex data like `arrays or structs`.

These functions are typically used with `LATERAL VIEW`, but some (like `inline()`) can be used directly in `SELECT`.

Flattening array of structs into rows+cols.

## 📌 Common Inline Generator Functions

Function| Description
explode()| Converts an array/map into multiple rows
posexplode()| Like explode, but includes position (index)
inline() |Flattens an array of structs into multiple rows with multiple columns
stack() |Unpivots static column values into rows
json_tuple()| Extracts values from a JSON string into columns
inline_outer() |Like inline, but includes NULL for empty arrays

## 🧠 inline() – In-Depth Example

### ✅ 1. Flatten Array of Structs into Multiple Rows & Columns

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1, ARRAY(NAMED_STRUCT('product', 'book', 'qty', 2),
            NAMED_STRUCT('product', 'pen', 'qty', 5)))
AS orders(order_id, items);
SELECT order_id, product, qty
FROM orders
LATERAL VIEW INLINE(items) AS product, qty;
```

### ✅ 2. inline_outer() – Keep NULL if array is empty

```sql
CREATE OR REPLACE TEMP VIEW orders_null AS
SELECT * FROM VALUES
  (1, ARRAY()),
  (2, ARRAY(NAMED_STRUCT('product', 'pen', 'qty', 5)))
AS orders(order_id, items);
SELECT order_id, product, qty
FROM orders_null
LATERAL VIEW INLINE_OUTER(items) AS product, qty;
```
