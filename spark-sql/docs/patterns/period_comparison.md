# :material-chart-line: Period Comparison

Compare metrics across periods — year-over-year (YoY), month-over-month (MoM), and week-over-week — using `LAG`, self-joins, and `DATE_TRUNC`.

---

## :material-toy-brick: Sample Data

```sql
-- monthly_revenue — one row per region per month
CREATE OR REPLACE TEMP VIEW monthly_revenue AS
SELECT * FROM VALUES
  ('APAC',  DATE '2023-01-01', 42000.00),
  ('APAC',  DATE '2023-02-01', 38500.00),
  ('APAC',  DATE '2023-03-01', 51200.00),
  ('APAC',  DATE '2023-04-01', 47800.00),
  ('APAC',  DATE '2024-01-01', 55000.00),
  ('APAC',  DATE '2024-02-01', 43200.00),
  ('APAC',  DATE '2024-03-01', 61500.00),
  ('APAC',  DATE '2024-04-01', 52900.00),
  ('EMEA',  DATE '2023-01-01', 31000.00),
  ('EMEA',  DATE '2023-02-01', 29500.00),
  ('EMEA',  DATE '2023-03-01', 35000.00),
  ('EMEA',  DATE '2023-04-01', 33000.00),
  ('EMEA',  DATE '2024-01-01', 38500.00),
  ('EMEA',  DATE '2024-02-01', 31000.00),
  ('EMEA',  DATE '2024-03-01', 40200.00),
  ('EMEA',  DATE '2024-04-01', 36800.00)
AS t(region, month, revenue);
```

| region | month | revenue |
|--------|-------|---------|
| APAC | 2023-01-01 | 42000.00 |
| APAC | 2024-01-01 | 55000.00 |
| EMEA | 2023-01-01 | 31000.00 |
| EMEA | 2024-01-01 | 38500.00 |
| … | … | … |

---

## :material-numeric-1-circle: Pattern 1 — Year-over-Year with LAG

`LAG` with an offset of 12 (months) retrieves the same month from the previous year when data is ordered chronologically per region.

```sql
WITH ordered AS (
    SELECT
        region,
        month,
        revenue,
        YEAR(month)  AS yr,
        MONTH(month) AS mo,
        LAG(revenue, 4) OVER (
            PARTITION BY region
            ORDER BY month
        ) AS prev_year_revenue          -- 4 months back = same month, prior year (given 4 months of data per year)
    FROM monthly_revenue
)
SELECT
    region,
    month,
    revenue,
    prev_year_revenue,
    ROUND((revenue - prev_year_revenue) / prev_year_revenue * 100, 1) AS yoy_pct
FROM ordered
WHERE prev_year_revenue IS NOT NULL
ORDER BY region, month;
-- Result:
-- region | month      | revenue  | prev_year_revenue | yoy_pct
-- -------|------------|----------|-------------------|--------
-- APAC   | 2024-01-01 | 55000.00 | 42000.00          | 31.0
-- APAC   | 2024-02-01 | 43200.00 | 38500.00          | 12.2
-- APAC   | 2024-03-01 | 61500.00 | 51200.00          | 20.1
-- APAC   | 2024-04-01 | 52900.00 | 47800.00          | 10.7
-- EMEA   | 2024-01-01 | 38500.00 | 31000.00          | 24.2
-- EMEA   | 2024-02-01 | 31000.00 | 29500.00          |  5.1
-- EMEA   | 2024-03-01 | 40200.00 | 35000.00          | 14.9
-- EMEA   | 2024-04-01 | 36800.00 | 33000.00          | 11.5
```

---

## :material-numeric-2-circle: Pattern 2 — YoY via self-join (robust for sparse data)

Self-join on matching `MONTH` and `YEAR - 1` handles sparse datasets where `LAG` offsets are unreliable.

```sql
SELECT
    cur.region,
    cur.month                                                        AS current_month,
    cur.revenue                                                      AS current_revenue,
    py.revenue                                                       AS prior_year_revenue,
    ROUND((cur.revenue - py.revenue) / py.revenue * 100, 1)         AS yoy_pct,
    ROUND(cur.revenue - py.revenue, 2)                               AS yoy_delta
FROM monthly_revenue AS cur
JOIN monthly_revenue AS py
    ON  cur.region     = py.region
    AND MONTH(cur.month) = MONTH(py.month)
    AND YEAR(cur.month)  = YEAR(py.month) + 1
ORDER BY cur.region, cur.month;
-- Result: (same as LAG version — self-join is equivalent for dense data)
-- region | current_month | current_revenue | prior_year_revenue | yoy_pct | yoy_delta
-- -------|---------------|-----------------|-------------------|---------|----------
-- APAC   | 2024-01-01    | 55000.00        | 42000.00           |  31.0   | 13000.00
-- APAC   | 2024-02-01    | 43200.00        | 38500.00           |  12.2   |  4700.00
-- APAC   | 2024-03-01    | 61500.00        | 51200.00           |  20.1   | 10300.00
-- APAC   | 2024-04-01    | 52900.00        | 47800.00           |  10.7   |  5100.00
```

---

## :material-numeric-3-circle: Pattern 3 — Month-over-Month with LAG(1)

```sql
SELECT
    region,
    month,
    revenue,
    LAG(revenue, 1) OVER (PARTITION BY region ORDER BY month)  AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue, 1) OVER (PARTITION BY region ORDER BY month))
        / LAG(revenue, 1) OVER (PARTITION BY region ORDER BY month) * 100,
    1) AS mom_pct
FROM monthly_revenue
ORDER BY region, month;
-- Result (APAC):
-- region | month      | revenue  | prev_month_revenue | mom_pct
-- -------|------------|----------|--------------------|--------
-- APAC   | 2023-01-01 | 42000.00 | NULL               |  NULL
-- APAC   | 2023-02-01 | 38500.00 | 42000.00           |  -8.3
-- APAC   | 2023-03-01 | 51200.00 | 38500.00           |  33.0
-- APAC   | 2023-04-01 | 47800.00 | 51200.00           |  -6.6
-- APAC   | 2024-01-01 | 55000.00 | 47800.00           |  15.1  ← crosses year boundary
```

!!! warning "Cross-year MoM"
    `LAG(1) OVER (PARTITION BY region ORDER BY month)` does not know about year
    boundaries. If `2023-04` and `2024-01` are consecutive rows, the MoM for Jan 2024
    compares against Apr 2023 — not the prior month. Add `PARTITION BY region, YEAR(month)`
    or filter to continuous monthly sequences.

---

## :material-numeric-4-circle: Pattern 4 — Running YTD vs same YTD prior year

```sql
WITH ytd AS (
    SELECT
        region,
        YEAR(month)  AS yr,
        MONTH(month) AS mo,
        revenue,
        SUM(revenue) OVER (
            PARTITION BY region, YEAR(month)
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ytd_revenue
    FROM monthly_revenue
)
SELECT
    cur.region,
    cur.mo         AS month_num,
    cur.yr         AS current_year,
    cur.ytd_revenue,
    py.ytd_revenue AS prior_ytd_revenue,
    ROUND((cur.ytd_revenue - py.ytd_revenue) / py.ytd_revenue * 100, 1) AS ytd_yoy_pct
FROM ytd AS cur
JOIN ytd AS py
    ON cur.region = py.region
   AND cur.mo     = py.mo
   AND cur.yr     = py.yr + 1
ORDER BY cur.region, cur.mo;
-- Result (APAC):
-- region | month_num | current_year | ytd_revenue | prior_ytd_revenue | ytd_yoy_pct
-- -------|-----------|--------------|-------------|-------------------|------------
-- APAC   | 1         | 2024         | 55000.00    | 42000.00           |  31.0
-- APAC   | 2         | 2024         | 98200.00    | 80500.00           |  22.0
-- APAC   | 3         | 2024         | 159700.00   | 131700.00          |  21.3
-- APAC   | 4         | 2024         | 212600.00   | 179500.00          |  18.4
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Same-month comparison (dense data) | `LAG(n) OVER (PARTITION BY region ORDER BY month)` |
| Same-month comparison (sparse / irregular data) | Self-join on `MONTH()` + `YEAR() - 1` |
| Rolling MoM within a year | `LAG(1) OVER (PARTITION BY region, YEAR(month) ORDER BY month)` |
| YTD cumulative vs prior YTD | `SUM OVER (PARTITION BY region, YEAR)` + self-join on month number |
| % growth with NULL safety | Wrap denominator in `NULLIF(prev_value, 0)` to avoid division by zero |
