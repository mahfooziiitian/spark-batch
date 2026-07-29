# :material-arrow-left-right: Navigation Functions

Navigation functions access values from other rows relative to the current row within a window partition.

---

## :material-pin: Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| `LAG` | `LAG(col [, offset [, default]]) OVER (...)` | Returns the value `offset` rows *before* the current row |
| `LEAD` | `LEAD(col [, offset [, default]]) OVER (...)` | Returns the value `offset` rows *after* the current row |
| `FIRST_VALUE` | `FIRST_VALUE(col) [IGNORE NULLS] OVER (...)` | Returns the first value in the window frame |
| `LAST_VALUE` | `LAST_VALUE(col) [IGNORE NULLS] OVER (...)` | Returns the last value in the window frame |
| `NTH_VALUE` | `NTH_VALUE(col, n) [IGNORE NULLS] OVER (...)` | Returns the `n`-th value in the window frame (1-based index) |

---

## :material-magnify: Behavior

1. **Default offset for LAG/LEAD**: `offset` defaults to `1` when omitted — `LAG(amount)` is equivalent to `LAG(amount, 1)`.
2. **Default value for LAG/LEAD**: the third argument provides a fallback when the offset reaches beyond the partition boundary (e.g., the first row has no preceding row for `LAG`). Defaults to `NULL` when not specified.
3. **LAST_VALUE frame requirement**: the default frame is `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, so without an explicit frame `LAST_VALUE` returns the *current* row's value. Always specify `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` to scan the full partition.
4. **NTH_VALUE frame requirement**: same as `LAST_VALUE` — always provide an explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame to ensure the full partition is scanned.
5. **IGNORE NULLS**: `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE` support `IGNORE NULLS` (placed after the closing parenthesis in Spark 4) to skip `NULL` values when scanning the frame.
6. **Frame clause and LAG/LEAD**: `LAG` and `LEAD` do **not** accept a frame clause — specifying one causes a parse error.

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
FROM sales
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | prev_sale | next_sale |
    |-------|------------|-------:|----------:|----------:|
    | Alice | 2024-01-01 |    100 |      NULL |       200 |
    | Alice | 2024-01-05 |    200 |       100 |       300 |
    | Alice | 2024-01-10 |    300 |       200 |      NULL |
    | Bob   | 2024-01-02 |    150 |      NULL |       250 |
    | Bob   | 2024-01-06 |    250 |       150 |      NULL |
    | Carol | 2024-01-03 |    400 |      NULL |       500 |
    | Carol | 2024-01-07 |    500 |       400 |      NULL |

    - First row per rep → `prev_sale` is NULL (no preceding row).
    - Last row per rep → `next_sale` is NULL (no following row).

### Example 2 — Delta Calculation (Change from Previous)

```sql
SELECT
    rep,
    sale_date,
    amount,
    amount - LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date) AS diff_from_last,
    ROUND(
        100.0 * (amount - LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date))
              / LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date), 1
    ) AS pct_change
FROM sales
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | diff_from_last | pct_change |
    |-------|------------|-------:|---------------:|-----------:|
    | Alice | 2024-01-01 |    100 |           NULL |       NULL |
    | Alice | 2024-01-05 |    200 |            100 |      100.0 |
    | Alice | 2024-01-10 |    300 |            100 |       50.0 |
    | Bob   | 2024-01-02 |    150 |           NULL |       NULL |
    | Bob   | 2024-01-06 |    250 |            100 |       66.7 |
    | Carol | 2024-01-03 |    400 |           NULL |       NULL |
    | Carol | 2024-01-07 |    500 |            100 |       25.0 |

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
FROM sales
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | first_sale | last_sale |
    |-------|------------|-------:|-----------:|----------:|
    | Alice | 2024-01-01 |    100 |        100 |       300 |
    | Alice | 2024-01-05 |    200 |        100 |       300 |
    | Alice | 2024-01-10 |    300 |        100 |       300 |
    | Bob   | 2024-01-02 |    150 |        150 |       250 |
    | Bob   | 2024-01-06 |    250 |        150 |       250 |
    | Carol | 2024-01-03 |    400 |        400 |       500 |
    | Carol | 2024-01-07 |    500 |        400 |       500 |

    - `first_sale` — always the opening sale amount for each rep.
    - `last_sale` — always the most recent sale (requires full-partition frame).

!!! warning "LAST_VALUE without explicit frame"
    Without `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`,
    `LAST_VALUE` returns the **current row** — making it seem like a no-op:
    ```sql
    -- ✗ Returns current row's amount (misleading)
    LAST_VALUE(amount) OVER (PARTITION BY rep ORDER BY sale_date)
    -- ✓ Returns the actual last value in the partition
    LAST_VALUE(amount) OVER (PARTITION BY rep ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
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
FROM sales
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | second_sale |
    |-------|------------|-------:|------------:|
    | Alice | 2024-01-01 |    100 |         200 |
    | Alice | 2024-01-05 |    200 |         200 |
    | Alice | 2024-01-10 |    300 |         200 |
    | Bob   | 2024-01-02 |    150 |         250 |
    | Bob   | 2024-01-06 |    250 |         250 |
    | Carol | 2024-01-03 |    400 |         500 |
    | Carol | 2024-01-07 |    500 |         500 |

    `NTH_VALUE` returns NULL when the partition has fewer than `n` rows.

### Example 5 — LAG with Default Value

Replace the `NULL` on the first row with a fallback of `0`:

```sql
SELECT
    rep,
    sale_date,
    amount,
    LAG(amount, 1, 0) OVER (PARTITION BY rep ORDER BY sale_date) AS prev_sale
FROM sales
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | prev_sale |
    |-------|------------|-------:|----------:|
    | Alice | 2024-01-01 |    100 |         0 |
    | Alice | 2024-01-05 |    200 |       100 |
    | Alice | 2024-01-10 |    300 |       200 |
    | Bob   | 2024-01-02 |    150 |         0 |
    | Bob   | 2024-01-06 |    250 |       150 |
    | Carol | 2024-01-03 |    400 |         0 |
    | Carol | 2024-01-07 |    500 |       400 |

    The default `0` replaces NULL, making downstream arithmetic safe
    (no need for `COALESCE`).

### Example 6 — Multi-Step LAG (Look Back N Rows)

Compare each sale to the one **2 sales ago** using `LAG(col, 2)`:

```sql
SELECT
    rep,
    sale_date,
    amount,
    LAG(amount, 1) OVER (PARTITION BY rep ORDER BY sale_date) AS prev_1,
    LAG(amount, 2) OVER (PARTITION BY rep ORDER BY sale_date) AS prev_2
FROM sales
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | prev_1 | prev_2 |
    |-------|------------|-------:|-------:|-------:|
    | Alice | 2024-01-01 |    100 |   NULL |   NULL |
    | Alice | 2024-01-05 |    200 |    100 |   NULL |
    | Alice | 2024-01-10 |    300 |    200 |    100 |
    | Bob   | 2024-01-02 |    150 |   NULL |   NULL |
    | Bob   | 2024-01-06 |    250 |    150 |   NULL |
    | Carol | 2024-01-03 |    400 |   NULL |   NULL |
    | Carol | 2024-01-07 |    500 |    400 |   NULL |

    - `LAG(amount, 2)` returns NULL when fewer than 2 preceding rows exist.
    - Useful for comparing to 2 periods ago, 3 periods ago, etc.

---

## :material-flask-outline: Scenario Examples

### Scenario 1 — Detecting Direction Changes (Trend Reversal)

Flag rows where the trend changes from increasing to decreasing or vice versa:

```sql
CREATE OR REPLACE TEMP VIEW stock_prices AS
SELECT * FROM VALUES
  ('ACME', '2024-01-01', 100.0),
  ('ACME', '2024-01-02', 105.0),
  ('ACME', '2024-01-03', 110.0),
  ('ACME', '2024-01-04', 108.0),  -- reversal: was going up, now down
  ('ACME', '2024-01-05', 103.0),
  ('ACME', '2024-01-06', 107.0)   -- reversal: was going down, now up
AS stock_prices(ticker, trade_date, price);

WITH deltas AS (
    SELECT
        ticker,
        trade_date,
        price,
        price - LAG(price) OVER (PARTITION BY ticker ORDER BY trade_date) AS delta,
        LAG(price) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_price
    FROM stock_prices
)
SELECT
    ticker,
    trade_date,
    price,
    delta,
    LAG(delta) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_delta,
    CASE
        WHEN delta > 0 AND LAG(delta) OVER (PARTITION BY ticker ORDER BY trade_date) < 0 THEN 'REVERSAL UP'
        WHEN delta < 0 AND LAG(delta) OVER (PARTITION BY ticker ORDER BY trade_date) > 0 THEN 'REVERSAL DOWN'
        ELSE ''
    END AS signal
FROM deltas
ORDER BY trade_date;
```

??? success "Expected Output"

    | ticker | trade_date | price | delta | prev_delta | signal        |
    |--------|------------|------:|------:|-----------:|---------------|
    | ACME   | 2024-01-01 | 100.0 |  NULL |       NULL |               |
    | ACME   | 2024-01-02 | 105.0 |   5.0 |       NULL |               |
    | ACME   | 2024-01-03 | 110.0 |   5.0 |        5.0 |               |
    | ACME   | 2024-01-04 | 108.0 |  -2.0 |        5.0 | REVERSAL DOWN |
    | ACME   | 2024-01-05 | 103.0 |  -5.0 |       -2.0 |               |
    | ACME   | 2024-01-06 | 107.0 |   4.0 |       -5.0 | REVERSAL UP   |

    Uses two layers of `LAG`: first to compute `delta`, then to compare
    consecutive deltas for sign changes.

### Scenario 2 — Time Between Events (Inter-Event Duration)

Calculate how many days elapsed between each customer order:

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  ('C001', '2024-01-05', 250),
  ('C001', '2024-01-12', 180),
  ('C001', '2024-02-01', 320),
  ('C001', '2024-03-15', 150),
  ('C002', '2024-01-08', 400),
  ('C002', '2024-01-10', 200)
AS orders(customer_id, order_date, amount);

SELECT
    customer_id,
    order_date,
    amount,
    LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date,
    DATEDIFF(
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)
    ) AS days_since_last_order
FROM orders
ORDER BY customer_id, order_date;
```

??? success "Expected Output"

    | customer_id | order_date | amount | prev_order_date | days_since_last_order |
    |-------------|------------|-------:|-----------------|----------------------:|
    | C001        | 2024-01-05 |    250 | NULL            |                  NULL |
    | C001        | 2024-01-12 |    180 | 2024-01-05      |                     7 |
    | C001        | 2024-02-01 |    320 | 2024-01-12      |                    20 |
    | C001        | 2024-03-15 |    150 | 2024-02-01      |                    43 |
    | C002        | 2024-01-08 |    400 | NULL            |                  NULL |
    | C002        | 2024-01-10 |    200 | 2024-01-08      |                     2 |

    Useful for churn prediction — customers with increasing gaps may be at risk.

### Scenario 3 — Opening and Closing Values (Financial Reporting)

Get the first and last price of each month for a stock:

```sql
CREATE OR REPLACE TEMP VIEW daily_prices AS
SELECT * FROM VALUES
  ('ACME', '2024-01-02', 100.0),
  ('ACME', '2024-01-15', 108.0),
  ('ACME', '2024-01-31', 112.0),
  ('ACME', '2024-02-01', 111.0),
  ('ACME', '2024-02-14', 115.0),
  ('ACME', '2024-02-28', 120.0)
AS daily_prices(ticker, trade_date, price);

SELECT
    ticker,
    DATE_TRUNC('month', trade_date) AS month,
    trade_date,
    price,
    FIRST_VALUE(price) OVER (
        PARTITION BY ticker, DATE_TRUNC('month', trade_date)
        ORDER BY trade_date
    ) AS month_open,
    LAST_VALUE(price) OVER (
        PARTITION BY ticker, DATE_TRUNC('month', trade_date)
        ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS month_close
FROM daily_prices
ORDER BY ticker, trade_date;
```

??? success "Expected Output"

    | ticker | month      | trade_date | price | month_open | month_close |
    |--------|------------|------------|------:|-----------:|------------:|
    | ACME   | 2024-01-01 | 2024-01-02 | 100.0 |      100.0 |       112.0 |
    | ACME   | 2024-01-01 | 2024-01-15 | 108.0 |      100.0 |       112.0 |
    | ACME   | 2024-01-01 | 2024-01-31 | 112.0 |      100.0 |       112.0 |
    | ACME   | 2024-02-01 | 2024-02-01 | 111.0 |      111.0 |       120.0 |
    | ACME   | 2024-02-01 | 2024-02-14 | 115.0 |      111.0 |       120.0 |
    | ACME   | 2024-02-01 | 2024-02-28 | 120.0 |      111.0 |       120.0 |

    - `FIRST_VALUE` doesn't need an explicit frame (default scans from start).
    - `LAST_VALUE` **requires** the full-partition frame to see the closing price.

### Scenario 4 — Accessing a Specific Row (NTH_VALUE for Benchmarking)

Compare each employee's salary to the 2nd highest in their department:

```sql
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  ('Engineering', 'Alice', 110000),
  ('Engineering', 'Bob',    95000),
  ('Engineering', 'Carol', 102000),
  ('Engineering', 'Dave',   88000),
  ('Sales',       'Eve',    80000),
  ('Sales',       'Frank',  72000),
  ('Sales',       'Grace',  75000)
AS employees(dept, name, salary);

SELECT
    dept,
    name,
    salary,
    NTH_VALUE(salary, 2) OVER (
        PARTITION BY dept ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS second_highest,
    salary - NTH_VALUE(salary, 2) OVER (
        PARTITION BY dept ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS diff_from_2nd
FROM employees
ORDER BY dept, salary DESC;
```

??? success "Expected Output"

    | dept        | name  | salary | second_highest | diff_from_2nd |
    |-------------|-------|-------:|---------------:|--------------:|
    | Engineering | Alice | 110000 |         102000 |          8000 |
    | Engineering | Carol | 102000 |         102000 |             0 |
    | Engineering | Bob   |  95000 |         102000 |         -7000 |
    | Engineering | Dave  |  88000 |         102000 |        -14000 |
    | Sales       | Eve   |  80000 |          75000 |          5000 |
    | Sales       | Grace |  75000 |          75000 |             0 |
    | Sales       | Frank |  72000 |          75000 |         -3000 |

    `NTH_VALUE(salary, 2)` with `ORDER BY salary DESC` gives the
    second-highest salary — a useful benchmark for the department.

### Scenario 5 — Forward-Fill with IGNORE NULLS

Carry the last known status forward through NULL gaps:

```sql
CREATE OR REPLACE TEMP VIEW device_status AS
SELECT * FROM VALUES
  ('D1', '2024-01-01 08:00', 'ONLINE'),
  ('D1', '2024-01-01 09:00', NULL),
  ('D1', '2024-01-01 10:00', NULL),
  ('D1', '2024-01-01 11:00', 'OFFLINE'),
  ('D1', '2024-01-01 12:00', NULL),
  ('D1', '2024-01-01 13:00', 'ONLINE')
AS device_status(device_id, ts, status);

SELECT
    device_id,
    ts,
    status,
    LAST_VALUE(status) IGNORE NULLS OVER (
        PARTITION BY device_id
        ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS effective_status
FROM device_status
ORDER BY device_id, ts;
```

??? success "Expected Output"

    | device_id | ts               | status  | effective_status |
    |-----------|------------------|---------|------------------|
    | D1        | 2024-01-01 08:00 | ONLINE  | ONLINE           |
    | D1        | 2024-01-01 09:00 | NULL    | ONLINE           |
    | D1        | 2024-01-01 10:00 | NULL    | ONLINE           |
    | D1        | 2024-01-01 11:00 | OFFLINE | OFFLINE          |
    | D1        | 2024-01-01 12:00 | NULL    | OFFLINE          |
    | D1        | 2024-01-01 13:00 | ONLINE  | ONLINE           |

    `IGNORE NULLS` skips NULL entries and returns the most recent
    non-null status — the classic forward-fill pattern.

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Compare each row to the previous / next row | `LAG` / `LEAD` |
| Calculate period-over-period change | `amount - LAG(amount) OVER (...)` |
| Detect trend reversals | Compare `LAG(delta)` sign to current delta |
| Time between events | `DATEDIFF(date, LAG(date) OVER (...))` |
| Retrieve opening or closing value per group | `FIRST_VALUE` / `LAST_VALUE` with explicit frame |
| Access a specific ranked row's value | `NTH_VALUE(col, n)` with full-partition frame |
| Avoid NULL on boundary rows | `LAG(col, 1, default_value)` |
| Forward-fill missing values | `LAST_VALUE(col) IGNORE NULLS` with frame to current row |
