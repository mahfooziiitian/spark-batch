# :material-arrow-left-right: Navigation Functions

Navigation functions access values from other rows relative to the current row within a window partition.

---

## :material-pin: Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| `LAG` | `LAG(col [, offset [, default]]) OVER (...)` | Returns the value `offset` rows *before* the current row |
| `LEAD` | `LEAD(col [, offset [, default]]) OVER (...)` | Returns the value `offset` rows *after* the current row |
| `FIRST_VALUE` | `FIRST_VALUE(col [IGNORE NULLS]) OVER (...)` | Returns the first value in the window frame |
| `LAST_VALUE` | `LAST_VALUE(col [IGNORE NULLS]) OVER (...)` | Returns the last value in the window frame |
| `NTH_VALUE` | `NTH_VALUE(col, n [IGNORE NULLS]) OVER (...)` | Returns the `n`-th value in the window frame (1-based index) |

---

## :material-magnify: Behavior

1. **Default offset for LAG/LEAD**: `offset` defaults to `1` when omitted — `LAG(amount)` is equivalent to `LAG(amount, 1)`.
2. **Default value for LAG/LEAD**: the third argument provides a fallback when the offset reaches beyond the partition boundary (e.g., the first row has no preceding row for `LAG`). Defaults to `NULL` when not specified.
3. **LAST_VALUE frame requirement**: the default frame is `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, so without an explicit frame `LAST_VALUE` returns the *current* row's value. Always specify `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` to scan the full partition.
4. **NTH_VALUE frame requirement**: same as `LAST_VALUE` — always provide an explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame to ensure the full partition is scanned.
5. **IGNORE NULLS**: `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE` support `IGNORE NULLS` to skip `NULL` values when scanning the frame.

---

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', '2024-01-01', 100),
  ('North', 'Alice', '2024-01-05', 200),
  ('North', 'Alice', '2024-01-10', 300),
  ('North', 'Bob',   '2024-01-02', 150),
  ('North', 'Bob',   '2024-01-06', 250),
  ('South', 'Carol', '2024-01-03', 400),
  ('South', 'Carol', '2024-01-07', 500)
AS sales(region, rep, sale_date, amount);
```

### Example 1 — LAG and LEAD Side-by-Side

```sql
SELECT
    rep,
    sale_date,
    amount,
    LAG(amount)  OVER (PARTITION BY rep ORDER BY sale_date) AS prev_sale,
    LEAD(amount) OVER (PARTITION BY rep ORDER BY sale_date) AS next_sale
FROM sales;
-- Result:
-- | rep   | sale_date  | amount | prev_sale | next_sale |
-- |-------|------------|--------|-----------|-----------|
-- | Alice | 2024-01-01 |    100 |      NULL |       200 |
-- | Alice | 2024-01-05 |    200 |       100 |       300 |
-- | Alice | 2024-01-10 |    300 |       200 |      NULL |
-- | Bob   | 2024-01-02 |    150 |      NULL |       250 |
-- | Bob   | 2024-01-06 |    250 |       150 |      NULL |
-- | Carol | 2024-01-03 |    400 |      NULL |       500 |
-- | Carol | 2024-01-07 |    500 |       400 |      NULL |
```

### Example 2 — Delta Calculation

```sql
SELECT
    rep,
    sale_date,
    amount,
    amount - LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date) AS diff_from_last
FROM sales;
-- Result:
-- | rep   | sale_date  | amount | diff_from_last |
-- |-------|------------|--------|----------------|
-- | Alice | 2024-01-01 |    100 |           NULL |
-- | Alice | 2024-01-05 |    200 |            100 |
-- | Alice | 2024-01-10 |    300 |            100 |
-- | Bob   | 2024-01-02 |    150 |           NULL |
-- | Bob   | 2024-01-06 |    250 |            100 |
-- | Carol | 2024-01-03 |    400 |           NULL |
-- | Carol | 2024-01-07 |    500 |            100 |
```

### Example 3 — FIRST_VALUE and LAST_VALUE

Without an explicit frame `LAST_VALUE` returns the current row; specify `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` to look at the full partition:

```sql
SELECT
    rep,
    sale_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY rep ORDER BY sale_date
    ) AS first_sale,
    LAST_VALUE(amount) OVER (
        PARTITION BY rep ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_sale
FROM sales;
-- Result:
-- | rep   | sale_date  | amount | first_sale | last_sale |
-- |-------|------------|--------|------------|-----------|
-- | Alice | 2024-01-01 |    100 |        100 |       300 |
-- | Alice | 2024-01-05 |    200 |        100 |       300 |
-- | Alice | 2024-01-10 |    300 |        100 |       300 |
-- | Bob   | 2024-01-02 |    150 |        150 |       250 |
-- | Bob   | 2024-01-06 |    250 |        150 |       250 |
-- | Carol | 2024-01-03 |    400 |        400 |       500 |
-- | Carol | 2024-01-07 |    500 |        400 |       500 |
```

### Example 4 — NTH_VALUE

Retrieve the 2nd sale amount for each rep, ordered by date:

```sql
SELECT
    rep,
    sale_date,
    amount,
    NTH_VALUE(amount, 2) OVER (
        PARTITION BY rep ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS second_sale
FROM sales;
-- Result:
-- | rep   | sale_date  | amount | second_sale |
-- |-------|------------|--------|-------------|
-- | Alice | 2024-01-01 |    100 |         200 |
-- | Alice | 2024-01-05 |    200 |         200 |
-- | Alice | 2024-01-10 |    300 |         200 |
-- | Bob   | 2024-01-02 |    150 |         250 |
-- | Bob   | 2024-01-06 |    250 |         250 |
-- | Carol | 2024-01-03 |    400 |         500 |
-- | Carol | 2024-01-07 |    500 |         500 |
-- NTH_VALUE returns NULL when the partition has fewer than n rows.
```

### Example 5 — LAG with Default Value

Replace the `NULL` on the first row with a fallback of `0`:

```sql
SELECT
    rep,
    sale_date,
    amount,
    LAG(amount, 1, 0) OVER (PARTITION BY rep ORDER BY sale_date) AS prev_sale
FROM sales;
-- Result:
-- | rep   | sale_date  | amount | prev_sale |
-- |-------|------------|--------|-----------|
-- | Alice | 2024-01-01 |    100 |         0 |
-- | Alice | 2024-01-05 |    200 |       100 |
-- | Alice | 2024-01-10 |    300 |       200 |
-- | Bob   | 2024-01-02 |    150 |         0 |
-- | Bob   | 2024-01-06 |    250 |       150 |
-- | Carol | 2024-01-03 |    400 |         0 |
-- | Carol | 2024-01-07 |    500 |       400 |
```

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Compare each row to the previous / next row | `LAG` / `LEAD` |
| Calculate period-over-period change | `amount - LAG(amount) OVER (...)` |
| Retrieve opening or closing value per group | `FIRST_VALUE` / `LAST_VALUE` with explicit frame |
| Access a specific ranked row's value | `NTH_VALUE(col, n)` with full-partition frame |
| Avoid NULL on boundary rows | `LAG(col, 1, default_value)` |
