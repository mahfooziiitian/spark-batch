# Count Functions

Count functions determine the number of rows or distinct values in a group.

## 📌 Functions

| Function | Description | Returns |
|----------|-------------|---------|
| `COUNT(*)` | Count all rows (including NULLs) | `BIGINT` |
| `COUNT(expr)` | Count non-NULL values | `BIGINT` |
| `COUNT(DISTINCT expr)` | Count distinct non-NULL values | `BIGINT` |
| `COUNT_IF(condition)` | Count rows where condition is true | `BIGINT` |
| `APPROX_COUNT_DISTINCT(expr)` | Approximate distinct count (faster, ~2% error) | `BIGINT` |
| `APPROX_PERCENTILE(col, pct, accuracy)` | Approximate percentile value | Same as `col` |

## 🧪 Practical Examples

### COUNT Variants

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(col) AS non_null_count,
  COUNT(DISTINCT col) AS distinct_count
FROM VALUES (1), (1), (2), (NULL), (3) AS tab(col);
-- total_rows=5, non_null_count=4, distinct_count=3
```

### COUNT_IF — Conditional Counting

```sql
SELECT
  count_if(col % 2 = 0) AS even_count,
  count_if(col IS NULL) AS null_count
FROM VALUES (NULL), (0), (1), (2), (3) AS tab(col);
-- even_count=2, null_count=1
```

### APPROX_COUNT_DISTINCT — Fast Estimation

```sql
SELECT approx_count_distinct(col) AS approx_distinct
FROM VALUES (1), (1), (2), (2), (3) AS tab(col);
-- Result: 3 (approximate, uses HyperLogLog)
```

### APPROX_PERCENTILE

```sql
SELECT approx_percentile(col, array(0.25, 0.5, 0.75), 100)
FROM VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS tab(col);
-- Result: [2, 5, 7]
```

### Grouped Counting

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  ('East', 'completed'), ('East', 'cancelled'),
  ('West', 'completed'), ('West', 'completed'),
  ('East', 'completed'), ('West', 'cancelled')
AS orders(region, status);

SELECT
  region,
  COUNT(*) AS total,
  count_if(status = 'completed') AS completed,
  count_if(status = 'cancelled') AS cancelled
FROM orders
GROUP BY region;
```

| region | total | completed | cancelled |
|--------|-------|-----------|-----------|
| East | 3 | 2 | 1 |
| West | 3 | 2 | 1 |

## 🧠 COUNT vs APPROX_COUNT_DISTINCT

| Function | Exact | Speed | Use Case |
|----------|-------|-------|----------|
| `COUNT(DISTINCT col)` | Yes | Slower (shuffle) | Small-medium data |
| `APPROX_COUNT_DISTINCT(col)` | ~2% error | Much faster | Large-scale cardinality |
