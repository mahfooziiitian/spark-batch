# posexplode

Explode with Index.

## What It Does

posexplode() takes an array or map and:

1. Returns one row per element
2. Adds the position (index) of each element
3. Works similarly to explode(), but includes the index

## 📌 Syntax

```sql
POSEXPLODE(array_or_map)
```

## Usage examples

### 1. Explode an Array with Index

```sql
CREATE OR REPLACE TEMP VIEW people AS
SELECT * FROM VALUES
  (1, ARRAY('Alice', 'Bob', 'Charlie')),
  (2, ARRAY('Diana', 'Eve'))
AS people(id, names);
SELECT id, pos, name
FROM people
LATERAL VIEW POSEXPLODE(names) AS pos, name;
```

### 2. With Array of Structs

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1001, ARRAY(NAMED_STRUCT('product', 'book', 'qty', 2),
               NAMED_STRUCT('product', 'pen', 'qty', 5)))
AS orders(order_id, products);
SELECT order_id, pos, item.product AS product, item.qty AS qty
FROM orders
LATERAL VIEW POSEXPLODE(products) AS pos, item;
```

### 3. Label Choices with Index

```sql
CREATE OR REPLACE TEMP VIEW survey AS
SELECT * FROM VALUES
  (101, ARRAY('A', 'B', 'C'))
AS survey(user_id, answers);
SELECT user_id, CONCAT('Q', pos + 1) AS question, answer
FROM survey
LATERAL VIEW POSEXPLODE(answers) AS pos, answer;
```

### 4. Compare with explode()

```sql
-- explode()
select val  from LATERAL VIEW EXPLODE(array('x', 'y')) AS val;

-- posexplode()
select pos, val from LATERAL VIEW POSEXPLODE(array('x', 'y')) AS pos, val;
```

### 🧠 Use with sequence()

```sql
SELECT date_add('2024-01-01', pos) AS date
FROM (
  SELECT posexplode(sequence(1, 5)) AS (pos, _) 
) t;
```

## ✅ Summary: When to Use posexplode()

Use Case| Why posexplode() Helps
---|---
Track order or position in array| Adds index column
Label dynamically (e.g., Q1, Q2…)| Use index to generate field names
Exploding with sequence metadata| Combine index with logic
Work with array of structs |Index + fields like product, qty
