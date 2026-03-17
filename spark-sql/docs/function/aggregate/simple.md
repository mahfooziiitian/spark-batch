# Simple Aggregate Functions

Simple aggregate functions perform basic numeric computations across rows in a group.

## 📌 Functions

| Function | Description | NULL Handling |
|----------|-------------|---------------|
| `MIN(expr)` | Minimum value | Ignores NULLs |
| `MAX(expr)` | Maximum value | Ignores NULLs |
| `SUM(expr)` | Sum of all values | Ignores NULLs |
| `AVG(expr)` | Arithmetic mean | Ignores NULLs |
| `MEAN(expr)` | Alias for `AVG` | Ignores NULLs |
| `MIN_BY(x, y)` | Value of `x` at the row where `y` is minimum | Ignores NULLs in `y` |
| `MAX_BY(x, y)` | Value of `x` at the row where `y` is maximum | Ignores NULLs in `y` |
| `ANY_VALUE(expr)` | Any arbitrary value from the group | May return NULL |

## 🧪 Practical Examples

### MIN / MAX

```sql
SELECT MIN(col), MAX(col) FROM VALUES (10), (50), (20) AS tab(col);
-- Result: 10, 50
```

### SUM / AVG

```sql
SELECT SUM(col), AVG(col) FROM VALUES (10), (50), (20) AS tab(col);
-- Result: 80, 26.666...
```

### MAX_BY — Value at Maximum

```sql
SELECT max_by(x, y) FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS tab(x, y);
-- Result: 'b' (the x value where y is maximum)
```

### MIN_BY — Value at Minimum

```sql
SELECT min_by(x, y) FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS tab(x, y);
-- Result: 'a' (the x value where y is minimum)
```

### NULL Handling

```sql
SELECT SUM(col), AVG(col), COUNT(col)
FROM VALUES (10), (NULL), (20) AS tab(col);
-- SUM=30, AVG=15.0, COUNT=2 (NULLs excluded)
```

### Grouped Aggregation

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('East', 100), ('East', 200), ('West', 150),
  ('West', 300), ('East', 50)
AS sales(region, amount);

SELECT
  region,
  COUNT(*) AS num_sales,
  SUM(amount) AS total,
  AVG(amount) AS avg_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
FROM sales
GROUP BY region;
```

| region | num_sales | total | avg_amount | min_amount | max_amount |
|--------|-----------|-------|------------|------------|------------|
| East | 3 | 350 | 116.67 | 50 | 200 |
| West | 2 | 450 | 225.0 | 150 | 300 |

## 🧠 When to Use

| Need | Function |
|------|----------|
| Find extremes | `MIN`, `MAX` |
| Total a column | `SUM` |
| Average a column | `AVG` / `MEAN` |
| Value associated with min/max | `MIN_BY`, `MAX_BY` |
| Any sample value from group | `ANY_VALUE` |
