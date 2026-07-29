# :material-view-grid: Window Functions

Window functions compute a value for each row using a **sliding or bounded set of rows
— the *window* — without collapsing the result set** the way `GROUP BY` does.
Every input row is preserved in the output.

!!! tip "Window vs GROUP BY"
    `GROUP BY` produces **one row per group**. Window functions produce
    **one value per row** while still being able to reference other rows in the partition.

---

## :material-head-question: When to Use Window Functions

| Use Case | Example |
|----------|---------|
| Ranking / top-N per group | Top 3 products per category by revenue |
| Running totals & averages | Cumulative sales per region over time |
| Row-to-row comparison | Compare each sale to the previous one (`LAG`) |
| Percent of total | Each row's contribution to its group total |
| De-duplication | Keep only the latest record per entity |
| Gap detection | Find missing sequence numbers or time gaps |
| Moving averages | 7-day rolling average of daily metrics |

---

## :material-sitemap: How Window Functions Work

```mermaid
flowchart TD
    INPUT["Input rows\n(all rows preserved)"] --> PART["PARTITION BY\nsplit into logical groups"]
    PART --> SORT["ORDER BY\norder rows within each group"]
    SORT --> FRAME["Frame clause\nROWS or RANGE\ndefines which rows to include"]
    FRAME --> FUNC["Window function\nranking / aggregate / navigation"]
    FUNC --> OUT["One output value\nper input row"]
```

---

## :material-animation-play: Interactive Demo

### Row-by-Row Computation

Select a window function and click any row to see how it computes a result
using its partition and ordering context.

<div id="viz-window-concept" class="ts-viz"></div>

### PARTITION BY Visualizer

Each partition is processed independently — the animation highlights how the
window scans rows within each group.

<div id="viz-partition-demo" class="ts-viz"></div>

---

## :material-code-braces: Full Syntax

```sql
function_name([expression]) OVER (
    [PARTITION BY partition_col [, ...]]
    [ORDER BY    sort_col [ASC | DESC] [NULLS FIRST | NULLS LAST] [, ...]]
    [{ ROWS | RANGE } BETWEEN frame_start AND frame_end]
)
```

Frame boundary keywords:

```
UNBOUNDED PRECEDING   -- start of partition
N PRECEDING           -- N rows / N value-units before current row
CURRENT ROW           -- current row
N FOLLOWING           -- N rows / N value-units after current row
UNBOUNDED FOLLOWING   -- end of partition
```

---

## :material-table: Clause Reference

| Clause | Required | Default | Purpose |
|--------|----------|---------|---------|
| `PARTITION BY` | No | Entire result set = one partition | Resets the window per distinct combination of partition columns |
| `ORDER BY` | Depends | — | Defines row sequence within each partition; required for ranking and navigation |
| `ROWS / RANGE BETWEEN` | No | See below | Limits which rows in the partition are included in the aggregate |

**Default frame when `ORDER BY` is present:** `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`  
**Default frame when `ORDER BY` is absent:** `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` (full partition)

---

## :material-compare: Function Categories

| Category | Functions | Requires ORDER BY | Respects Frame |
|----------|-----------|:-----------------:|:--------------:|
| Ranking | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK` | Yes | No — ignores frame silently |
| Aggregate | `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `CUME_DIST` | Optional | Yes |
| Navigation | `LAG`, `LEAD` | Yes | No — **errors** if frame specified |
| Navigation | `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE` | Yes | Yes |

---

## :material-window-open: Named Windows (WINDOW Clause)

When multiple window functions share the same `OVER (...)` specification,
define the window once with a `WINDOW` clause and reference it by name.
This avoids repetition and ensures consistency.

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', '2024-01-01', 100),
  ('North', 'Alice', '2024-01-05', 200),
  ('North', 'Bob',   '2024-01-02', 150),
  ('South', 'Carol', '2024-01-03', 400),
  ('South', 'Carol', '2024-01-07', 500)
AS sales(region, rep, sale_date, amount);

SELECT
    region,
    rep,
    sale_date,
    amount,
    ROW_NUMBER()  OVER w       AS row_num,
    SUM(amount)   OVER w       AS running_total,
    AVG(amount)   OVER w       AS running_avg,
    -- LAG cannot use a framed window — reference the base window instead
    LAG(amount)   OVER (PARTITION BY region ORDER BY sale_date) AS prev_amount
FROM sales
WINDOW w AS (
    PARTITION BY region
    ORDER BY sale_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
ORDER BY region, sale_date;
```

!!! warning "Navigation functions reject frame clauses"
    `LAG`, `LEAD`, `ROW_NUMBER`, `RANK`, and `DENSE_RANK` ignore or reject frame
    specifications. If your named window includes a frame, navigation functions must
    use a separate `OVER(...)` without one.

??? success "Expected Output"

    | region | rep   | sale_date  | amount | row_num | running_total | running_avg | prev_amount |
    |--------|-------|------------|-------:|--------:|--------------:|------------:|------------:|
    | North  | Alice | 2024-01-01 |    100 |       1 |           100 |       100.0 |        NULL |
    | North  | Bob   | 2024-01-02 |    150 |       2 |           250 |       125.0 |         100 |
    | North  | Alice | 2024-01-05 |    200 |       3 |           450 |       150.0 |         150 |
    | South  | Carol | 2024-01-03 |    400 |       1 |           400 |       400.0 |        NULL |
    | South  | Carol | 2024-01-07 |    500 |       2 |           900 |       450.0 |         400 |

Multiple named windows let you share the `PARTITION BY` while varying
`ORDER BY` and frame clauses across functions:

```sql
SELECT
    region,
    rep,
    sale_date,
    amount,
    -- Running total with ordering and frame
    SUM(amount) OVER w_running AS running,
    -- Full partition total (no ORDER BY, full frame)
    SUM(amount) OVER w_total   AS region_total
FROM sales
WINDOW
    w_total   AS (PARTITION BY region),
    w_running AS (PARTITION BY region ORDER BY sale_date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY region, sale_date;
```

??? success "Expected Output"

    | region | rep   | sale_date  | amount | running | region_total |
    |--------|-------|------------|-------:|--------:|-------------:|
    | North  | Alice | 2024-01-01 |    100 |     100 |          450 |
    | North  | Bob   | 2024-01-02 |    150 |     250 |          450 |
    | North  | Alice | 2024-01-05 |    200 |     450 |          450 |
    | South  | Carol | 2024-01-03 |    400 |     400 |          900 |
    | South  | Carol | 2024-01-07 |    500 |     900 |          900 |

!!! note "Spark does not support window inheritance"
    Unlike PostgreSQL, Spark SQL cannot extend a named window with additional clauses
    (e.g., `OVER (w ORDER BY ...)`). Define separate named windows instead.

---

## :material-check-all: QUALIFY — Filter on Window Results

`QUALIFY` filters rows based on a window function result **without needing a subquery**.

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', '2024-01-01', 100),
  ('North', 'Alice', '2024-01-05', 200),
  ('North', 'Bob',   '2024-01-02', 150),
  ('North', 'Bob',   '2024-01-06', 300),
  ('South', 'Carol', '2024-01-03', 400),
  ('South', 'Carol', '2024-01-07', 500)
AS sales(region, rep, sale_date, amount);

-- Keep only the top sale per rep (equivalent to ROW_NUMBER subquery, but cleaner)
SELECT region, rep, sale_date, amount
FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY rep ORDER BY amount DESC) = 1;
```

??? success "Expected Output"

    | region | rep   | sale_date  | amount |
    |--------|-------|------------|-------:|
    | North  | Alice | 2024-01-05 |    200 |
    | North  | Bob   | 2024-01-06 |    300 |
    | South  | Carol | 2024-01-07 |    500 |

```sql
-- Keep rows where the amount exceeds the partition average
SELECT region, rep, sale_date, amount
FROM sales
QUALIFY amount > AVG(amount) OVER (PARTITION BY region);
```

??? success "Expected Output"

    | region | rep   | sale_date  | amount |
    |--------|-------|------------|-------:|
    | North  | Alice | 2024-01-05 |    200 |
    | North  | Bob   | 2024-01-06 |    300 |
    | South  | Carol | 2024-01-07 |    500 |

    North average = 187.5, South average = 450 — only rows above their partition average survive.

```sql
-- Keep the 3 most recent sales per region
SELECT region, rep, sale_date, amount
FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date DESC) <= 3;
```

??? success "Expected Output"

    | region | rep   | sale_date  | amount |
    |--------|-------|------------|-------:|
    | North  | Bob   | 2024-01-06 |    300 |
    | North  | Alice | 2024-01-05 |    200 |
    | North  | Bob   | 2024-01-02 |    150 |
    | South  | Carol | 2024-01-07 |    500 |
    | South  | Carol | 2024-01-03 |    400 |

!!! note "QUALIFY vs subquery"
    `QUALIFY` is cleaner but requires Spark 3.3+ or Databricks Runtime. For older
    versions, wrap the window function in a subquery and filter with `WHERE`.

---

## :material-speedometer: Performance Notes

| Practice | Reason |
|----------|--------|
| Minimise distinct `OVER` clauses | Each unique `OVER` spec causes a separate shuffle stage |
| Use named `WINDOW` for shared specs | Avoids duplicate shuffles for identical windows |
| Prefer `ROWS` over `RANGE` | `ROWS` uses row position — faster; `RANGE` requires value comparison |
| Add `PARTITION BY` when possible | Smaller partitions = less data per task, better parallelism |
| Avoid window functions on raw large tables | Pre-filter / aggregate first, then apply window |
| Check EXPLAIN for `Exchange` nodes | Each shuffle = one window stage; too many = slow |

```sql
-- Check how many Exchange nodes a windowed query generates
EXPLAIN FORMATTED
SELECT
    region, rep, sale_date, amount,
    SUM(amount) OVER (PARTITION BY region ORDER BY sale_date
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rnk
FROM sales;
-- Two different OVER specs → two separate Exchange (shuffle) stages
```

---

## :material-flask-outline: Quick Example — All Three Categories

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', '2024-01-01', 100),
  ('North', 'Alice', '2024-01-05', 200),
  ('North', 'Alice', '2024-01-10', 300),
  ('North', 'Bob',   '2024-01-02', 150),
  ('North', 'Bob',   '2024-01-06', 300),
  ('South', 'Carol', '2024-01-03', 400),
  ('South', 'Carol', '2024-01-07', 500)
AS sales(region, rep, sale_date, amount);

SELECT
    region,
    rep,
    sale_date,
    amount,
    -- Ranking
    RANK()       OVER (PARTITION BY region ORDER BY amount DESC) AS rnk,
    -- Aggregate
    SUM(amount)  OVER (PARTITION BY region ORDER BY sale_date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    ROUND(amount * 100.0 /
          SUM(amount) OVER (PARTITION BY region), 1)             AS pct_of_region,
    -- Navigation
    LAG(amount)  OVER (PARTITION BY rep ORDER BY sale_date)       AS prev_amount
FROM sales
ORDER BY region, sale_date;
```

??? success "Expected Output"

    | region | rep   | sale_date  | amount | rnk | running_total | pct_of_region | prev_amount |
    |--------|-------|------------|-------:|----:|--------------:|--------------:|------------:|
    | North  | Alice | 2024-01-01 |    100 |   4 |           100 |           9.5 |        NULL |
    | North  | Bob   | 2024-01-02 |    150 |   3 |           250 |          14.3 |        NULL |
    | North  | Alice | 2024-01-05 |    200 |   2 |           450 |          19.0 |         100 |
    | North  | Bob   | 2024-01-06 |    300 |   1 |           750 |          28.6 |         150 |
    | North  | Alice | 2024-01-10 |    300 |   1 |          1050 |          28.6 |         200 |
    | South  | Carol | 2024-01-03 |    400 |   2 |           400 |          44.4 |        NULL |
    | South  | Carol | 2024-01-07 |    500 |   1 |           900 |          55.6 |         400 |

    **How to read this:**

    - `rnk` — RANK within each region by amount descending (ties share rank).
    - `running_total` — cumulative sum within each region ordered by date.
    - `pct_of_region` — each row's contribution to the region's total amount.
    - `prev_amount` — previous sale amount for the same rep (NULL for first row).

---

## :material-book-open-variant: In This Section

| Page | Contents |
|------|----------|
| [Window Types](functions/index.md) | Ranking, aggregate, navigation — full function reference |
| [Frame](frame/index.md) | ROWS vs RANGE, boundary syntax, default frames |
| [Application](application/index.md) | De-duplication, top-N, running balance, sessionisation, percentile |
| [NULL Options](nulls/null_options_wf.md) | `IGNORE NULLS`, `RESPECT NULLS`, `NULLS FIRST / LAST` |

---

## :material-compare-horizontal: Window Functions vs Alternatives

| Approach | Preserves Rows | Cross-row Access | Performance |
|----------|:--------------:|:----------------:|-------------|
| `GROUP BY` | :material-close: | :material-close: | Fast — single pass |
| Correlated subquery | :material-check: | :material-check: | Slow — row-by-row |
| Self-join | :material-check: | :material-check: | Expensive shuffle |
| **Window function** | :material-check: | :material-check: | Efficient — single shuffle per unique `OVER` spec |

---

## :material-lightbulb-outline: Common Patterns Cheat Sheet

```sql
CREATE OR REPLACE TEMP VIEW users AS
SELECT * FROM VALUES
  (1, 'alice@ex.com', '2024-01-01 10:00:00'),
  (1, 'alice@new.com', '2024-01-05 14:00:00'),
  (2, 'bob@ex.com',   '2024-01-02 09:00:00')
AS users(user_id, email, updated_at);

-- De-duplicate: keep latest per key
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) AS rn
    FROM users
) WHERE rn = 1;
```

??? success "Expected Output"

    | user_id | email         | updated_at          | rn |
    |--------:|---------------|---------------------|----|
    |       1 | alice@new.com | 2024-01-05 14:00:00 |  1 |
    |       2 | bob@ex.com    | 2024-01-02 09:00:00 |  1 |

```sql
CREATE OR REPLACE TEMP VIEW transactions AS
SELECT * FROM VALUES
  ('A001', '2024-01-01', 500),
  ('A001', '2024-01-02', 300),
  ('A001', '2024-01-03', -200),
  ('A001', '2024-01-04', 150)
AS transactions(account, txn_date, amount);

-- Running total
SELECT
    account,
    txn_date,
    amount,
    SUM(amount) OVER (PARTITION BY account ORDER BY txn_date ROWS UNBOUNDED PRECEDING) AS balance
FROM transactions;
```

??? success "Expected Output"

    | account | txn_date   | amount | balance |
    |---------|------------|-------:|--------:|
    | A001    | 2024-01-01 |    500 |     500 |
    | A001    | 2024-01-02 |    300 |     800 |
    | A001    | 2024-01-03 |   -200 |     600 |
    | A001    | 2024-01-04 |    150 |     750 |

```sql
CREATE OR REPLACE TEMP VIEW daily_metrics AS
SELECT * FROM VALUES
  ('2024-01-01', 10),
  ('2024-01-02', 20),
  ('2024-01-03', 30),
  ('2024-01-04', 40),
  ('2024-01-05', 50),
  ('2024-01-06', 60),
  ('2024-01-07', 70)
AS daily_metrics(event_date, value);

-- 7-day moving average
SELECT
    event_date,
    value,
    ROUND(AVG(value) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily_metrics;
```

??? success "Expected Output"

    | event_date | value | avg_7d |
    |------------|------:|-------:|
    | 2024-01-01 |    10 |   10.0 |
    | 2024-01-02 |    20 |   15.0 |
    | 2024-01-03 |    30 |   20.0 |
    | 2024-01-04 |    40 |   25.0 |
    | 2024-01-05 |    50 |   30.0 |
    | 2024-01-06 |    60 |   35.0 |
    | 2024-01-07 |    70 |   40.0 |

```sql
-- Percent of group total
SELECT
    region,
    rep,
    amount,
    ROUND(amount * 100.0 / SUM(amount) OVER (PARTITION BY region), 1) AS pct_of_region
FROM sales;
```

??? success "Expected Output (using sales view from above)"

    | region | rep   | amount | pct_of_region |
    |--------|-------|-------:|--------------:|
    | North  | Alice |    100 |           9.5 |
    | North  | Alice |    200 |          19.0 |
    | North  | Alice |    300 |          28.6 |
    | North  | Bob   |    150 |          14.3 |
    | North  | Bob   |    300 |          28.6 |
    | South  | Carol |    400 |          44.4 |
    | South  | Carol |    500 |          55.6 |

```sql
-- Previous and next row
SELECT
    account,
    txn_date,
    amount,
    LAG(amount, 1)  OVER (PARTITION BY account ORDER BY txn_date) AS prev_amount,
    LEAD(amount, 1) OVER (PARTITION BY account ORDER BY txn_date) AS next_amount
FROM transactions;
```

??? success "Expected Output"

    | account | txn_date   | amount | prev_amount | next_amount |
    |---------|------------|-------:|------------:|------------:|
    | A001    | 2024-01-01 |    500 |        NULL |         300 |
    | A001    | 2024-01-02 |    300 |         500 |        -200 |
    | A001    | 2024-01-03 |   -200 |         300 |         150 |
    | A001    | 2024-01-04 |    150 |        -200 |        NULL |
