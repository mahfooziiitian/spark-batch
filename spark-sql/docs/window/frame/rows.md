# :material-table-row: ROWS Frame

`ROWS` frames define the window using a **physical row offset** from the current row. Each row is treated independently regardless of its ORDER BY value.

---

## :material-pin: Syntax

```sql
ROWS BETWEEN frame_start AND frame_end
```

---

## :material-magnify: Behavior

1. **Physical offset**: the frame boundary is a fixed number of rows above or below the current row in the ordered partition — not a value distance.
2. **Frame shrinks at partition edges**: when the current row is near the start or end of a partition, the frame silently contracts to what is available (no error, no padding with NULL).
3. **Tie ordering**: `ROWS` treats tied rows independently, so if two rows share the same `ORDER BY` value their running totals will differ. Use a unique `ORDER BY` key (e.g., a surrogate id or timestamp) when determinism matters.

---

## :material-flask-outline: Examples

```sql
CREATE OR REPLACE TEMP VIEW daily_sales AS
SELECT * FROM VALUES
  ('2024-01-01', 100),
  ('2024-01-02', 200),
  ('2024-01-03', 150),
  ('2024-01-04', 300),
  ('2024-01-05', 250)
AS daily_sales(sale_date, amount);
```

### Example 1 — Running Total

```sql
SELECT
    sale_date,
    amount,
    SUM(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM daily_sales
ORDER BY sale_date;
-- Result:
-- | sale_date  | amount | running_total |
-- |------------|--------|---------------|
-- | 2024-01-01 |    100 |           100 |
-- | 2024-01-02 |    200 |           300 |
-- | 2024-01-03 |    150 |           450 |
-- | 2024-01-04 |    300 |           750 |
-- | 2024-01-05 |    250 |          1000 |
```

### Example 2 — 3-Row Centred Moving Average

```sql
SELECT
    sale_date,
    amount,
    ROUND(AVG(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ), 2) AS moving_avg_3
FROM daily_sales
ORDER BY sale_date;
-- Result:
-- | sale_date  | amount | moving_avg_3 |
-- |------------|--------|--------------|
-- | 2024-01-01 |    100 |       150.00 |  -- frame: rows 1-2 only
-- | 2024-01-02 |    200 |       150.00 |
-- | 2024-01-03 |    150 |       216.67 |
-- | 2024-01-04 |    300 |       233.33 |
-- | 2024-01-05 |    250 |       275.00 |  -- frame: rows 4-5 only
```

### Example 3 — Trailing 2-Row Window

```sql
SELECT
    sale_date,
    amount,
    SUM(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS trailing_2_sum
FROM daily_sales
ORDER BY sale_date;
-- Result:
-- | sale_date  | amount | trailing_2_sum |
-- |------------|--------|----------------|
-- | 2024-01-01 |    100 |            100 |
-- | 2024-01-02 |    200 |            300 |
-- | 2024-01-03 |    150 |            450 |
-- | 2024-01-04 |    300 |            650 |
-- | 2024-01-05 |    250 |            700 |
```

### Example 4 — Future Lookahead

```sql
SELECT
    sale_date,
    amount,
    SUM(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ) AS next_2_sum
FROM daily_sales
ORDER BY sale_date;
-- Result:
-- | sale_date  | amount | next_2_sum |
-- |------------|--------|------------|
-- | 2024-01-01 |    100 |        450 |
-- | 2024-01-02 |    200 |        650 |
-- | 2024-01-03 |    150 |        700 |
-- | 2024-01-04 |    300 |        550 |
-- | 2024-01-05 |    250 |        250 |  -- only current row in frame
```

### Example 5 — Full Partition

```sql
SELECT
    sale_date,
    amount,
    SUM(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS grand_total
FROM daily_sales
ORDER BY sale_date;
-- Result:
-- | sale_date  | amount | grand_total |
-- |------------|--------|-------------|
-- | 2024-01-01 |    100 |        1000 |
-- | 2024-01-02 |    200 |        1000 |
-- | 2024-01-03 |    150 |        1000 |
-- | 2024-01-04 |    300 |        1000 |
-- | 2024-01-05 |    250 |        1000 |
-- Same value on every row — useful for percentage-of-total calculations.
```

---

## :material-brain: When to Use

| Scenario | Frame |
|----------|-------|
| Cumulative total from first row to current | `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| Centred N-row smoothing window | `ROWS BETWEEN N PRECEDING AND N FOLLOWING` |
| Trailing N-row window (e.g., last 3 sales) | `ROWS BETWEEN N PRECEDING AND CURRENT ROW` |
| Look ahead N rows | `ROWS BETWEEN CURRENT ROW AND N FOLLOWING` |
| Static partition total on every row | `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` |
