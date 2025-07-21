# explode

EXPLODE function in Spark SQL, which is essential for flattening arrays or maps in your data.

Flattening arrays/maps into rows.

## 🧠 What is EXPLODE() in Spark SQL?

EXPLODE() transforms a single row with an array or map into multiple rows, one per element.

## 📌 Syntax

### Generic

```sql
EXPLODE(array_or_map)
```

### Using LATERAL VIEW

```sql
SELECT ...
FROM your_table
LATERAL VIEW EXPLODE(array_or_map_column) AS element_column;
```

### For maps

```sql
LATERAL VIEW EXPLODE(map_col) AS key_col, value_col
```

### 🔧 Input Types

Input Type |Result
---|---
`array<T>`| multiple rows with T values
`map<K,V>`| multiple rows with key, value pairs

## 🔍 Behavior

1. Converts each element of an array into a new row.
2. Duplicates all other column values for each new row.
3. For maps, it splits into two columns: key and value.

## 🧪 Practical Examples

### 🧱 1. Exploding an Array of Primitives

```sql
CREATE OR REPLACE TEMP VIEW people AS
SELECT * FROM VALUES
  (1, ARRAY('Alice', 'Bob')),
  (2, ARRAY('Charlie', 'Diana'))
AS people(id, names);
SELECT id, name
FROM people
LATERAL VIEW EXPLODE(names) AS name;
```

### 🗺️ 2. Exploding a Map

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  (1, MAP('apple', 2, 'banana', 3)),
  (2, MAP('orange', 1, 'grape', 5))
AS sales(id, items);
SELECT id, fruit, quantity
FROM sales
LATERAL VIEW EXPLODE(items) AS fruit, quantity;
```

### 🧱 3. Exploding an Array of Structs

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1001, ARRAY(NAMED_STRUCT('product', 'book', 'qty', 2),
               NAMED_STRUCT('product', 'pen', 'qty', 5))),
  (1002, ARRAY(NAMED_STRUCT('product', 'notebook', 'qty', 1),
               NAMED_STRUCT('product', 'eraser', 'qty', 3)))
AS orders(order_id, products);
SELECT order_id, item.product AS product, item.qty AS quantity
FROM orders
LATERAL VIEW EXPLODE(products) AS item;
```

### 🔁 4. Explode with SEQUENCE() to Create Date Ranges

```sql
SELECT EXPLODE(SEQUENCE(DATE '2024-01-01', DATE '2024-01-05')) AS day;
```

### 🔗 5. Combine with TRANSFORM and FILTER

```sql
SELECT EXPLODE(
  FILTER(array(1, 2, 3, 4, 5), x -> x % 2 = 0)
) AS even;
```

## 🧠 When to Use EXPLODE

Use Case |Why Use EXPLODE()?
---|----
Flatten nested arrays |Turn 1 row with N values into N rows
Normalize JSON or semi-structured data| Easily unpack nested structures
Count elements or apply aggregations |Explode first, then use GROUP BY, etc.
Dynamic row generation |Use with SEQUENCE() for date/calendar logic
