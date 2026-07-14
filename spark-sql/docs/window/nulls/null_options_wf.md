# :material-null: NULL Options in Window Functions

Spark SQL lets you control whether `NULL` values are skipped or included when scanning a window frame, using the `IGNORE NULLS` / `RESPECT NULLS` clause, and control where `NULL` values appear in an ordered partition with `NULLS FIRST` / `NULLS LAST`.

---

## :material-pin: Syntax

```sql
-- NULL option on navigation functions
function_name(col [IGNORE NULLS | RESPECT NULLS]) OVER (...)

-- NULL placement in ORDER BY
ORDER BY col [ASC | DESC] [NULLS FIRST | NULLS LAST]
```

---

## :material-table: IGNORE NULLS Support

| Function | Supports IGNORE NULLS |
|----------|-----------------------|
| `LAG` | Yes |
| `LEAD` | Yes |
| `FIRST_VALUE` | Yes |
| `LAST_VALUE` | Yes |
| `NTH_VALUE` | Yes |
| `SUM` / `AVG` | No — NULLs are always excluded from arithmetic aggregates |
| `RANK` / `DENSE_RANK` | No — NULL is treated as a value and ranked |

---

## :material-magnify: Behavior

1. **`IGNORE NULLS`**: when scanning the frame, the function skips any row where the expression is `NULL` and continues to the next non-`NULL` row.
2. **`RESPECT NULLS` (default)**: `NULL` rows are included in the scan; the function returns `NULL` if the targeted row contains `NULL`.
3. **`NULLs` in `ORDER BY` — default placement**: in ascending order, `NULL` sorts last; in descending order, `NULL` sorts first.
4. **Override with `NULLS FIRST` / `NULLS LAST`**: you can explicitly place `NULL` at the beginning or end of the ordered partition regardless of sort direction — this affects the row positions seen by ranking and navigation functions.

---

## :material-flask-outline: Examples

```sql
CREATE OR REPLACE TEMP VIEW readings AS
SELECT * FROM VALUES
  ('Alice', '2024-01-01', 100),
  ('Alice', '2024-01-02', NULL),
  ('Alice', '2024-01-03', 200),
  ('Alice', '2024-01-04', NULL),
  ('Alice', '2024-01-05', 300)
AS readings(rep, reading_date, amount);
```

### Example 1 — FIRST_VALUE: IGNORE NULLS vs RESPECT NULLS

```sql
SELECT
    rep,
    reading_date,
    amount,
    FIRST_VALUE(amount IGNORE NULLS)  OVER (PARTITION BY rep ORDER BY reading_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_non_null,
    FIRST_VALUE(amount RESPECT NULLS) OVER (PARTITION BY rep ORDER BY reading_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_any
FROM readings
ORDER BY reading_date;
-- Result:
-- | rep   | reading_date | amount | first_non_null | first_any |
-- |-------|--------------|--------|----------------|-----------|
-- | Alice | 2024-01-01   |    100 |            100 |       100 |
-- | Alice | 2024-01-02   |   NULL |            100 |       100 |
-- | Alice | 2024-01-03   |    200 |            100 |       100 |
-- | Alice | 2024-01-04   |   NULL |            100 |       100 |
-- | Alice | 2024-01-05   |    300 |            100 |       100 |
-- FIRST_VALUE finds 100 on row 1; NULL rows make no difference here.
```

### Example 2 — LAST_VALUE IGNORE NULLS

Use a full-partition frame so `LAST_VALUE` scans beyond the current row.

```sql
SELECT
    rep,
    reading_date,
    amount,
    LAST_VALUE(amount IGNORE NULLS) OVER (
        PARTITION BY rep ORDER BY reading_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_non_null
FROM readings
ORDER BY reading_date;
-- Result:
-- | rep   | reading_date | amount | last_non_null |
-- |-------|--------------|--------|---------------|
-- | Alice | 2024-01-01   |    100 |           300 |
-- | Alice | 2024-01-02   |   NULL |           300 |
-- | Alice | 2024-01-03   |    200 |           300 |
-- | Alice | 2024-01-04   |   NULL |           300 |
-- | Alice | 2024-01-05   |    300 |           300 |
```

### Example 3 — LAG IGNORE NULLS

Skip back past `NULL` rows to find the previous non-null value.

```sql
SELECT
    rep,
    reading_date,
    amount,
    LAG(amount IGNORE NULLS) OVER (PARTITION BY rep ORDER BY reading_date) AS prev_non_null
FROM readings
ORDER BY reading_date;
-- Result:
-- | rep   | reading_date | amount | prev_non_null |
-- |-------|--------------|--------|---------------|
-- | Alice | 2024-01-01   |    100 |          NULL |  -- no preceding non-null
-- | Alice | 2024-01-02   |   NULL |           100 |
-- | Alice | 2024-01-03   |    200 |           100 |  -- skips the NULL on 01-02
-- | Alice | 2024-01-04   |   NULL |           200 |
-- | Alice | 2024-01-05   |    300 |           200 |  -- skips the NULL on 01-04
```

### Example 4 — LEAD IGNORE NULLS

Skip forward past `NULL` rows to find the next non-null value.

```sql
SELECT
    rep,
    reading_date,
    amount,
    LEAD(amount IGNORE NULLS) OVER (PARTITION BY rep ORDER BY reading_date) AS next_non_null
FROM readings
ORDER BY reading_date;
-- Result:
-- | rep   | reading_date | amount | next_non_null |
-- |-------|--------------|--------|---------------|
-- | Alice | 2024-01-01   |    100 |           200 |  -- skips NULL on 01-02
-- | Alice | 2024-01-02   |   NULL |           200 |
-- | Alice | 2024-01-03   |    200 |           300 |  -- skips NULL on 01-04
-- | Alice | 2024-01-04   |   NULL |           300 |
-- | Alice | 2024-01-05   |    300 |          NULL |  -- no following non-null
```

### Example 5 — NTH_VALUE IGNORE NULLS

Return the 2nd non-null amount in the partition.

```sql
SELECT
    rep,
    reading_date,
    amount,
    NTH_VALUE(amount IGNORE NULLS, 2) OVER (
        PARTITION BY rep ORDER BY reading_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS second_non_null
FROM readings
ORDER BY reading_date;
-- Result:
-- | rep   | reading_date | amount | second_non_null |
-- |-------|--------------|--------|-----------------|
-- | Alice | 2024-01-01   |    100 |             200 |
-- | Alice | 2024-01-02   |   NULL |             200 |
-- | Alice | 2024-01-03   |    200 |             200 |
-- | Alice | 2024-01-04   |   NULL |             200 |
-- | Alice | 2024-01-05   |    300 |             200 |
-- The 2nd non-null value is 200 (2024-01-03); NULL rows are skipped.
```

### Example 6 — NULL Placement in ORDER BY

Control where `NULL` rows appear in the partition, which changes the `ROW_NUMBER` assigned.

```sql
SELECT
    rep,
    reading_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY rep ORDER BY amount ASC NULLS FIRST)  AS rn_nulls_first,
    ROW_NUMBER() OVER (PARTITION BY rep ORDER BY amount ASC NULLS LAST)   AS rn_nulls_last
FROM readings
ORDER BY reading_date;
-- Result:
-- | rep   | reading_date | amount | rn_nulls_first | rn_nulls_last |
-- |-------|--------------|--------|----------------|---------------|
-- | Alice | 2024-01-01   |    100 |              3 |             1 |
-- | Alice | 2024-01-02   |   NULL |              1 |             4 |  -- NULL first vs last
-- | Alice | 2024-01-03   |    200 |              4 |             2 |
-- | Alice | 2024-01-04   |   NULL |              2 |             5 |  -- NULL first vs last
-- | Alice | 2024-01-05   |    300 |              5 |             3 |
```

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Forward-fill missing values | `LAST_VALUE(col IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| Backward-fill missing values | `FIRST_VALUE(col IGNORE NULLS) OVER (ORDER BY date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` |
| Get previous non-null reading | `LAG(col IGNORE NULLS) OVER (...)` |
| Get next non-null forecast | `LEAD(col IGNORE NULLS) OVER (...)` |
| Place NULLs at beginning of ranking | `ORDER BY col ASC NULLS FIRST` |
| Treat NULLs as low values (sort last) | `ORDER BY col ASC NULLS LAST` (default for ASC) |
