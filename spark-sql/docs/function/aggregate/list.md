# collect_list

`collect_list` collects values from a group into a list, preserving duplicates.

## 📌 Syntax

```sql
collect_list(expr)
```

- Returns: `ARRAY<T>` where `T` is the type of `expr`
- Preserves duplicates
- Non-deterministic ordering after shuffle

## 🔍 Behavior

1. Aggregates all non-NULL values from the group into an ordered array.
2. Duplicates are **preserved** (unlike `collect_set`).
3. The order of collected results depends on the order of rows, which may be non-deterministic after a shuffle.

## 🧪 Practical Examples

### Basic Collection

```sql
SELECT collect_list(col) FROM VALUES (1), (2), (1) AS tab(col);
-- Result: [1, 2, 1]
```

### Grouped Collection

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  ('Alice', 'laptop'), ('Alice', 'mouse'),
  ('Bob', 'keyboard'), ('Bob', 'monitor'), ('Bob', 'mouse')
AS orders(customer, product);

SELECT customer, collect_list(product) AS products
FROM orders
GROUP BY customer;
```

| customer | products |
|----------|----------|
| Alice | [laptop, mouse] |
| Bob | [keyboard, monitor, mouse] |

### NULL Handling

```sql
SELECT collect_list(col) FROM VALUES (1), (NULL), (3), (NULL) AS tab(col);
-- Result: [1, 3]  (NULLs are excluded)
```

### Combined with SORT_ARRAY

```sql
SELECT SORT_ARRAY(collect_list(col)) AS sorted
FROM VALUES (3), (1), (2), (1) AS tab(col);
-- Result: [1, 1, 2, 3]
```

## 🧠 collect_list vs collect_set

| Feature | collect_list | collect_set |
|---------|-------------|-------------|
| Duplicates | Preserved | Removed |
| NULLs | Excluded | Excluded |
| Use case | Ordered lists | Unique values |
