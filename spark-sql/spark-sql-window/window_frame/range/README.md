# Range

A RANGE frame defines the window based on the values of the ORDER BY column, rather than physical row positions.

1. Uses ORDER BY values
2. Time intervals, numeric gaps
3. RANGE requires numeric or date-based columns.

With RANGE BETWEEN 100 PRECEDING AND CURRENT ROW:
→ Includes all rows with amount ≥ current - 100

## ✅ Requirements for RANGE Frames

1. Must use a numeric, timestamp, or date column in ORDER BY.
2. You can use intervals (like INTERVAL 7 DAYS) or direct numeric values.

## Examples

### 7-Day Rolling Total (Using RANGE on Dates)

```sql

CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('2024-01-01', 'North', 100),
  ('2024-01-02', 'North', 200),
  ('2024-01-05', 'North', 300),
  ('2024-01-08', 'North', 400),
  ('2024-01-10', 'North', 500)
AS sales(sale_date, region, amount);
SELECT *,
  SUM(amount) OVER (
    ORDER BY to_date(sale_date)
    RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
  ) AS range_7d_total
FROM sales;
```

### Amount-Based Range (RANGE on Numeric Column)

Includes all rows where amount is within 200 units less than current row.

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('2024-01-01', 'North', 100),
  ('2024-01-02', 'North', 200),
  ('2024-01-05', 'North', 300),
  ('2024-01-08', 'North', 400),
  ('2024-01-10', 'North', 500)
AS sales(sale_date, region, amount);
SELECT *,
  SUM(amount) OVER (
    ORDER BY amount
    RANGE BETWEEN 200 PRECEDING AND CURRENT ROW
  ) AS range_amt_total
FROM sales;
```

### Invalid Use (Non-numeric ORDER BY in RANGE)

```sql
-- ❌ This will cause an error
SELECT *,
  SUM(amount) OVER (
    ORDER BY region
    RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
  )
FROM sales;
```

❌ region is a string → RANGE can’t be used.
