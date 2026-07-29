# :material-numeric-4-circle: Period-over-Period Comparison

Compute the sale-over-sale delta percentage relative to the previous sale for each rep.

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
    LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date) AS prev_amount,
    ROUND(
        100.0 * (amount - LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date))
              / LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date),
        1
    ) AS delta_pct
FROM sales
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | prev_amount | delta_pct |
    |-------|------------|-------:|------------:|----------:|
    | Alice | 2024-01-01 |    100 |        NULL |      NULL |
    | Alice | 2024-01-05 |    200 |         100 |     100.0 |
    | Alice | 2024-01-10 |    300 |         200 |      50.0 |
    | Bob   | 2024-01-02 |    150 |        NULL |      NULL |
    | Bob   | 2024-01-06 |    300 |         150 |     100.0 |
    | Carol | 2024-01-03 |    400 |        NULL |      NULL |
    | Carol | 2024-01-07 |    500 |         400 |      25.0 |

!!! warning "Repeated `LAG` calls"
    The query above calls `LAG(amount)` **three times** with the same `OVER` spec.
    Spark may or may not deduplicate them — and the repetition hurts readability.
    Use a **CTE** or **named WINDOW** to compute it once and reuse the result.

---

## :material-arrow-up-bold: Optimised Alternatives

### CTE Approach

Compute `LAG` once in a CTE, then derive the delta in the outer query:

```sql
WITH lagged AS (
    SELECT
        rep,
        sale_date,
        amount,
        LAG(amount) OVER (PARTITION BY rep ORDER BY sale_date) AS prev_amount
    FROM sales
)
SELECT
    rep,
    sale_date,
    amount,
    prev_amount,
    ROUND(100.0 * (amount - prev_amount) / prev_amount, 1) AS delta_pct
FROM lagged
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | prev_amount | delta_pct |
    |-------|------------|-------:|------------:|----------:|
    | Alice | 2024-01-01 |    100 |        NULL |      NULL |
    | Alice | 2024-01-05 |    200 |         100 |     100.0 |
    | Alice | 2024-01-10 |    300 |         200 |      50.0 |
    | Bob   | 2024-01-02 |    150 |        NULL |      NULL |
    | Bob   | 2024-01-06 |    300 |         150 |     100.0 |
    | Carol | 2024-01-03 |    400 |        NULL |      NULL |
    | Carol | 2024-01-07 |    500 |         400 |      25.0 |

    `LAG` is evaluated once in the CTE. The outer query references `prev_amount`
    as a plain column — no window function overhead.

### Named WINDOW Approach

Define the window once and reference it by name:

```sql
SELECT
    rep,
    sale_date,
    amount,
    LAG(amount) OVER w AS prev_amount,
    ROUND(
        100.0 * (amount - LAG(amount) OVER w) / LAG(amount) OVER w,
        1
    ) AS delta_pct
FROM sales
WINDOW w AS (PARTITION BY rep ORDER BY sale_date)
ORDER BY rep, sale_date;
```

??? success "Expected Output"

    | rep   | sale_date  | amount | prev_amount | delta_pct |
    |-------|------------|-------:|------------:|----------:|
    | Alice | 2024-01-01 |    100 |        NULL |      NULL |
    | Alice | 2024-01-05 |    200 |         100 |     100.0 |
    | Alice | 2024-01-10 |    300 |         200 |      50.0 |
    | Bob   | 2024-01-02 |    150 |        NULL |      NULL |
    | Bob   | 2024-01-06 |    300 |         150 |     100.0 |
    | Carol | 2024-01-03 |    400 |        NULL |      NULL |
    | Carol | 2024-01-07 |    500 |         400 |      25.0 |

    The named window ensures all `LAG` calls share the exact same spec — no risk of
    accidental mismatch. Spark recognises identical `OVER` specs and uses a single
    shuffle stage.

!!! tip "CTE vs Named WINDOW"
    | Approach | `LAG` evaluations | Readable | Best when |
    |----------|:-----------------:|:--------:|-----------|
    | CTE | 1 | :material-check: | Result is referenced multiple times in arithmetic |
    | Named WINDOW | N (but same shuffle) | :material-check: | Multiple different window functions share the same spec |

    **Prefer the CTE** when the window result feeds into expressions — it guarantees
    a single evaluation and keeps the outer query free of window syntax.

---

## :material-lightbulb-outline: When to Use

- Month-over-month or week-over-week growth metrics.
- Detecting sudden spikes or drops in a time series.
- Sales performance dashboards comparing current to previous period.

---

## :material-arrow-right: Related

- [YoY Comparison](yoy_comparison.md) — same concept across years with `LAG(col, 12)`
- [Window Types — Navigation](../functions/navigation.md) — `LAG`, `LEAD`, `FIRST_VALUE`
