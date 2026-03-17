# Explode

`EXPLODE()` transforms a single row containing an array or map into **multiple rows** — one per
element. It is the most commonly used generator function in Spark SQL.

## 📌 Syntax

### Direct

```sql
SELECT EXPLODE(array_or_map);
```

### With LATERAL VIEW

```sql
SELECT t.*, element
FROM your_table t
LATERAL VIEW EXPLODE(array_column) AS element;
```

### For Maps

```sql
SELECT t.*, key, value
FROM your_table t
LATERAL VIEW EXPLODE(map_column) AS key, value;
```

### Input Types

| Input Type | Output Columns |
|-----------|---------------|
| `array<T>` | `col` (element value) |
| `map<K,V>` | `key`, `value` |

## 🔍 Behavior

1. Produces one output row per array element or map entry.
2. Duplicates all other column values for each generated row.
3. For maps, each entry becomes a `(key, value)` row.
4. **Drops rows** where the array/map is `NULL` or empty — use `EXPLODE_OUTER` to retain them.
5. Default output column names are `col` (array) or `key`/`value` (map); override with `AS`.

## 🧪 Practical Examples

### 🧱 1. Explode an Array of Primitives

```sql
CREATE OR REPLACE TEMP VIEW people AS
SELECT * FROM VALUES
  (1, ARRAY('Alice', 'Bob')),
  (2, ARRAY('Charlie', 'Diana'))
AS people(id, names);

SELECT id, name
FROM people
LATERAL VIEW EXPLODE(names) AS name;
-- (1, Alice), (1, Bob), (2, Charlie), (2, Diana)
```

### 🗺️ 2. Explode a Map

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  (1, MAP('apple', 2, 'banana', 3)),
  (2, MAP('orange', 1, 'grape', 5))
AS sales(id, items);

SELECT id, fruit, quantity
FROM sales
LATERAL VIEW EXPLODE(items) AS fruit, quantity;
-- (1, apple, 2), (1, banana, 3), (2, orange, 1), (2, grape, 5)
```

### 🧱 3. Explode an Array of Structs

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1001, ARRAY(NAMED_STRUCT('product', 'book', 'qty', 2),
               NAMED_STRUCT('product', 'pen', 'qty', 5))),
  (1002, ARRAY(NAMED_STRUCT('product', 'notebook', 'qty', 1),
               NAMED_STRUCT('product', 'eraser', 'qty', 3)))
AS orders(order_id, products);

SELECT order_id, item.product, item.qty
FROM orders
LATERAL VIEW EXPLODE(products) AS item;
-- (1001, book, 2), (1001, pen, 5), (1002, notebook, 1), (1002, eraser, 3)
```

### 🔁 4. Generate Date Ranges with SEQUENCE

```sql
SELECT EXPLODE(SEQUENCE(DATE '2024-01-01', DATE '2024-01-05')) AS day;
-- 2024-01-01, 2024-01-02, …, 2024-01-05
```

### 🔗 5. Chain with Higher-Order Functions

```sql
-- Filter first, then explode
SELECT EXPLODE(
  FILTER(ARRAY(1, 2, 3, 4, 5), x -> x % 2 = 0)
) AS even;
-- 2, 4
```

### 🧱 6. Multiple LATERAL VIEWs

```sql
CREATE OR REPLACE TEMP VIEW multi AS
SELECT * FROM VALUES
  (1, ARRAY('a', 'b'), ARRAY(10, 20))
AS multi(id, letters, numbers);

SELECT id, letter, num
FROM multi
LATERAL VIEW EXPLODE(letters) AS letter
LATERAL VIEW EXPLODE(numbers) AS num;
-- Cross-product: (1,a,10), (1,a,20), (1,b,10), (1,b,20)
```

## 🧠 When to Use

| Scenario | Why `EXPLODE`? |
|----------|---------------|
| Flatten nested arrays into rows | Turn 1 row with N elements into N rows |
| Normalize JSON / semi-structured data | Unpack nested structures for analysis |
| Count or aggregate array elements | Explode first, then `GROUP BY` / `COUNT` |
| Generate date/number sequences | Combine with `SEQUENCE()` for calendar logic |
| Chain with HOFs | `EXPLODE(FILTER(...))` or `EXPLODE(TRANSFORM(...))` |

> **Tip:** If you need to preserve rows where the array is `NULL` or empty,
> use `EXPLODE_OUTER` instead.
