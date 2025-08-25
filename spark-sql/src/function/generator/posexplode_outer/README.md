# posexplode_outer

posexplode_outer() is like posexplode(), but it also returns rows for NULL or empty arrays, filling the position and value columns with NULL.

## 📌 Syntax

```sql
POSEXPLODE_OUTER(array_or_map)
```

Usage in SQL:

```sql
SELECT ...
FROM your_table
LATERAL VIEW POSEXPLODE_OUTER(array_column) AS pos, element;
```

## 🧪 Practical Examples

### 🔹 1. Basic Difference: posexplode() vs posexplode_outer()

#### Using posexplode() (doesn't return rows for empty/null)

```sql
CREATE OR REPLACE TEMP VIEW demo AS
SELECT * FROM VALUES
  (1, ARRAY('A', 'B')),
  (2, ARRAY()),          -- empty array
  (3, NULL)              -- NULL array
AS demo(id, arr);
SELECT id, pos, val
FROM demo
LATERAL VIEW POSEXPLODE(arr) AS pos, val;
```

#### 🔍 Using posexplode_outer()

```sql
CREATE OR REPLACE TEMP VIEW demo AS
SELECT * FROM VALUES
  (1, ARRAY('A', 'B')),
  (2, ARRAY()),          -- empty array
  (3, NULL)              -- NULL array
AS demo(id, arr);
SELECT id, pos, val
FROM demo
LATERAL VIEW POSEXPLODE_OUTER(arr) AS pos, val;
```

### 2. With Array of Structs

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (101, ARRAY(NAMED_STRUCT('item', 'book', 'qty', 2))),
  (102, ARRAY()),            -- Empty array
  (103, NULL)                -- NULL array
AS orders(order_id, products);
SELECT order_id, pos, p.item, p.qty
FROM orders
LATERAL VIEW POSEXPLODE_OUTER(products) AS pos, p;
```

### 3. Labeling Array Entries Safely

```sql
CREATE OR REPLACE TEMP VIEW students AS
SELECT * FROM VALUES
  (1, ARRAY('Math', 'Physics')),
  (2, NULL)
AS students(id, subjects);
SELECT id, CONCAT('Subject_', pos + 1) AS label, subject
FROM students
LATERAL VIEW POSEXPLODE_OUTER(subjects) AS pos, subject;
```

## ✅ Summary: posexplode_outer()

Use Case |Benefit
---|---
Arrays with NULL or empty |Still returns a row with NULLs
Joins, joins, joins!| Keeps outer rows intact after explode
Default-safe transformation| Avoids data loss from missing arrays
Consistent row output| Useful for reporting & visualization
