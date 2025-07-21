# 🧠 What is EXPLODE_OUTER()?

EXPLODE_OUTER() expands an array or map column into multiple rows, just like EXPLODE(), but it:

1. Retains rows even when the array/map is empty or NULL.
2. Outputs NULL for the exploded values in such cases.

## 📌 Syntax

```sql
EXPLODE_OUTER(array_or_map)
```

### With LATERAL VIEW

```sql
SELECT ...
FROM your_table
LATERAL VIEW EXPLODE_OUTER(array_column) AS value;
```

### For maps

```sql
LATERAL VIEW EXPLODE_OUTER(map_column) AS key, value;
```

## 🧪 Examples

### 🔹 1. Basic Difference: EXPLODE() vs EXPLODE_OUTER()

#### 🚫 Using EXPLODE() (drops NULL/empty)

```sql
CREATE OR REPLACE TEMP VIEW sample AS
SELECT * FROM VALUES
  (1, ARRAY('A', 'B')),
  (2, ARRAY()),
  (3, NULL)
AS sample(id, arr);
SELECT id, val
FROM sample
LATERAL VIEW EXPLODE(arr) AS val;

```

#### ✅ Using EXPLODE_OUTER()

➡️ EXPLODE_OUTER() preserves rows with empty and null arrays by filling NULL.

```sql
CREATE OR REPLACE TEMP VIEW sample AS
SELECT * FROM VALUES
  (1, ARRAY('A', 'B')),
  (2, ARRAY()),
  (3, NULL)
AS sample(id, arr);
SELECT id, val
FROM sample
LATERAL VIEW EXPLODE_OUTER(arr) AS val;

```

### 🔹 2. Exploding Array of Structs Safely

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1001, ARRAY(NAMED_STRUCT('product', 'pen', 'qty', 3))),
  (1002, ARRAY()),
  (1003, NULL)
AS orders(order_id, items);
SELECT order_id, item.product, item.qty
FROM orders
LATERAL VIEW EXPLODE_OUTER(items) AS item;
```

### 🔹 3. Exploding a Map with EXPLODE_OUTER

```sql
CREATE OR REPLACE TEMP VIEW inventory AS
SELECT * FROM VALUES
  (1, MAP('apple', 10, 'banana', 5)),
  (2, MAP()),
  (3, NULL)
AS inventory(id, stock);
SELECT id, fruit, quantity
FROM inventory
LATERAL VIEW EXPLODE_OUTER(stock) AS fruit, quantity;
```

### 🔹 4. Exploding with SEQUENCE() and Fallback

```sql
CREATE OR REPLACE TEMP VIEW ranges AS
SELECT * FROM VALUES
  (1, sequence(1, 3)),
  (2, ARRAY()),
  (3, NULL)
AS ranges(id, numbers);
SELECT id, num
FROM ranges
LATERAL VIEW EXPLODE_OUTER(numbers) AS num;
```

### 🧩 Bonus Tip: Use with IF, COALESCE

```sql
CREATE OR REPLACE TEMP VIEW sample AS
SELECT * FROM VALUES
  (1, ARRAY('A', 'B')),
  (2, ARRAY()),
  (3, NULL)
AS sample(id, arr);
SELECT id,
       COALESCE(val, 'no data') AS safe_val
FROM sample
LATERAL VIEW EXPLODE_OUTER(arr) AS val;
```

## ✅ When to Use EXPLODE_OUTER()

Situation| Why EXPLODE_OUTER() Helps
---|---
Keep all records (even null/empty)| Avoid losing rows on empty arrays/maps
Safe data unnesting |Good for survey, IoT, or optional fields
NULL/empty handling |Guarantees 1 row per original row
Required for outer joins| Combine with other tables while preserving base rows
