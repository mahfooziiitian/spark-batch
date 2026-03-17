# first / first_value

`first` returns the first value encountered in a group. When used with `ORDER BY`
in window functions, it returns the first value in the ordered frame.

## 📌 Syntax

```sql
first(expr[, ignoreNulls])
first_value(expr[, ignoreNulls])
```

- `expr`: The column or expression to evaluate
- `ignoreNulls`: When `true`, skips NULL values (default: `false`)
- Returns: Same type as `expr`

## 🔍 Behavior

1. Returns the first value encountered in the group.
2. Result is **non-deterministic** without an explicit `ORDER BY` (row order depends on shuffle).
3. When `ignoreNulls = true`, the first non-NULL value is returned.
4. `first` and `first_value` are aliases.

## 🧪 Practical Examples

### Basic Usage

```sql
SELECT first(col) FROM VALUES (10), (5), (20) AS tab(col);
-- Result: 10
```

### NULL Handling

```sql
-- Default: returns NULL if first value is NULL
SELECT first(col) FROM VALUES (NULL), (5), (20) AS tab(col);
-- Result: null

-- ignoreNulls = true: skips NULLs
SELECT first(col, true) FROM VALUES (NULL), (5), (20) AS tab(col);
-- Result: 5
```

### first_value (Alias)

```sql
SELECT first_value(col) FROM VALUES (10), (5), (20) AS tab(col);
-- Result: 10

SELECT first_value(col, true) FROM VALUES (NULL), (5), (20) AS tab(col);
-- Result: 5
```

### Window Function Usage

```sql
CREATE OR REPLACE TEMP VIEW events AS
SELECT * FROM VALUES
  ('Alice', '2024-01-01', NULL),
  ('Alice', '2024-01-02', 100),
  ('Alice', '2024-01-03', 300),
  ('Bob',   '2024-01-01', 200)
AS events(user_name, event_date, amount);

-- First non-null amount per user (ordered by date)
SELECT
  user_name,
  event_date,
  amount,
  FIRST_VALUE(amount, true) OVER (
    PARTITION BY user_name ORDER BY event_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS first_known_amount
FROM events;
```

## 🧠 first vs last

| Function | Returns | ignoreNulls |
|----------|---------|------------|
| `first(col)` | First value in group | Optional |
| `last(col)` | Last value in group | Optional |
| `first(col, true)` | First non-NULL value | Yes |
| `last(col, true)` | Last non-NULL value | Yes |
