# :material-numeric-3-circle: Running Balance

Compute a cumulative sales total per rep ordered chronologically.

---

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('Alice', '2024-01-01', 100),
  ('Alice', '2024-01-05', 200),
  ('Alice', '2024-01-10', 300),
  ('Bob',   '2024-01-02', 150),
  ('Bob',   '2024-01-06', 300),
  ('Carol', '2024-01-03', 400),
  ('Carol', '2024-01-07', 500)
AS sales(rep, sale_date, amount);

SELECT
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_balance
FROM sales
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | running_balance |
    |-------|------------|-------:|----------------:|
    | Alice | 2024-01-01 |    100 |             100 |
    | Alice | 2024-01-05 |    200 |             300 |
    | Alice | 2024-01-10 |    300 |             600 |
    | Bob   | 2024-01-02 |    150 |             150 |
    | Bob   | 2024-01-06 |    300 |             450 |
    | Carol | 2024-01-03 |    400 |             400 |
    | Carol | 2024-01-07 |    500 |             900 |

---

## :material-alert-outline: ROWS vs RANGE — Why the Frame Matters

When `ORDER BY` values contain **duplicates** (ties), the default `RANGE` frame
groups all tied rows together — giving them the same running total. `ROWS` processes
each row individually regardless of ties.

```sql
CREATE OR REPLACE TEMP VIEW daily_sales AS
SELECT * FROM VALUES
  ('Alice', '2024-01-01', 100),
  ('Alice', '2024-01-01', 250),  -- same date as previous row
  ('Alice', '2024-01-02', 300),
  ('Alice', '2024-01-02', 150)   -- same date as previous row
AS daily_sales(rep, sale_date, amount);

-- Default frame (RANGE): ties get the SAME cumulative value
SELECT
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY rep
        ORDER BY sale_date
        -- implicit: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_range
FROM daily_sales
ORDER BY rep, sale_date;
```

??? success "Expected Output — RANGE (default, problematic with ties)"

    | rep   | sale_date  | amount | running_range |
    |-------|------------|-------:|--------------:|
    | Alice | 2024-01-01 |    100 |           350 |
    | Alice | 2024-01-01 |    250 |           350 |
    | Alice | 2024-01-02 |    300 |           800 |
    | Alice | 2024-01-02 |    150 |           800 |

    Both Jan-01 rows show **350** (100 + 250) because `RANGE` treats all rows with
    the same `ORDER BY` value as peers — it includes the entire tied group up to
    the current value boundary.

```sql
-- Explicit ROWS frame: each row gets its own cumulative value
SELECT
    rep,
    sale_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY rep
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_rows
FROM daily_sales
ORDER BY rep, sale_date;
```

??? success "Expected Output — ROWS (deterministic, recommended)"

    | rep   | sale_date  | amount | running_rows |
    |-------|------------|-------:|-------------:|
    | Alice | 2024-01-01 |    100 |          100 |
    | Alice | 2024-01-01 |    250 |          350 |
    | Alice | 2024-01-02 |    300 |          650 |
    | Alice | 2024-01-02 |    150 |          800 |

    Each row accumulates independently. The order of tied rows is
    non-deterministic, but the final total is always correct.

!!! warning "Always use explicit `ROWS` for running balances"
    The default frame when `ORDER BY` is present is
    `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. This groups ties together,
    which is almost never what you want for a running balance. Always specify
    `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` explicitly.

!!! tip "Make ties deterministic"
    Add a tiebreaker column to `ORDER BY` (e.g., a unique transaction ID) so that
    even `ROWS` produces a predictable sequence:
    ```sql
    ORDER BY sale_date, txn_id
    ```

---

## :material-lightbulb-outline: When to Use

- Financial ledgers — running account balance after each transaction.
- Inventory tracking — cumulative stock in / stock out over time.
- Progress monitoring — cumulative completion percentage.

---

## :material-arrow-right: Related

- [Frame — ROWS](../frame/rows.md) — how `ROWS BETWEEN` boundaries work
- [Period-over-Period](period_comparison.md) — compare each row to the previous one
