# array_agg

`array_agg` collects values from a group into an array, including duplicates and NULLs.

## 📌 Syntax

```sql
array_agg(expr)
```

- Returns: `ARRAY<T>` where `T` is the type of `expr`
- Includes duplicates and NULL values
- Order of elements is non-deterministic after a shuffle

## 🔍 Behavior

1. Collects all values (including NULLs) from the group into an array.
2. Equivalent to `COLLECT_LIST` but follows SQL standard naming.
3. The result order is **non-deterministic** unless combined with `ORDER BY` in a window function.

## 🧪 Practical Examples

### Basic Aggregation

```sql
SELECT array_agg(col) FROM VALUES (1), (2), (1) AS tab(col);
-- Result: [1, 2, 1]
```

### Grouped Aggregation

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('East', 100), ('East', 200), ('West', 300),
  ('West', 100), ('East', 100)
AS sales(region, amount);

SELECT region, array_agg(amount) AS amounts
FROM sales
GROUP BY region;
```

| region | amounts |
|--------|---------|
| East | [100, 200, 100] |
| West | [300, 100] |

### With NULL Values

```sql
SELECT array_agg(col) FROM VALUES (1), (NULL), (3) AS tab(col);
-- Result: [1, null, 3]
```

### Distinct Values (Use collect_set Instead)

```sql
-- array_agg keeps duplicates; for distinct, use collect_set
SELECT collect_set(col) FROM VALUES (1), (2), (1) AS tab(col);
-- Result: [1, 2]
```

## 🧠 array_agg vs collect_list vs collect_set

| Function | Duplicates | NULLs | Standard |
|----------|-----------|-------|----------|
| `array_agg` | Kept | Included | SQL standard |
| `collect_list` | Kept | Included | Spark-specific |
| `collect_set` | Removed | Excluded | Spark-specific |
