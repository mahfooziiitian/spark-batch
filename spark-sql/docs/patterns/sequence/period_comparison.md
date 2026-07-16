# :material-chart-line: Period Comparison

Compare metrics across periods — year-over-year (YoY), month-over-month (MoM), week-over-week (WoW), and quarter-over-quarter — using `LAG`, self-joins, `DATE_TRUNC`, and `FILTER`.

---

## :material-sitemap: YoY Execution Flow

```mermaid
flowchart LR
    DATA["monthly_revenue\nregion · month · revenue"] --> PART["PARTITION BY region\nORDER BY month"]
    PART --> LAG["LAG(revenue, N)\nN = months between years"]
    LAG --> DELTA["(current − prior)\n÷ prior × 100"]
    DELTA --> YOY["YoY % change\nper region per month"]
```

---

## :material-database: Sample Data

### Dataset 1: Monthly revenue by region (2 years)

```sql
CREATE OR REPLACE TEMP VIEW monthly_revenue AS
SELECT * FROM VALUES
  ('APAC',  DATE '2023-01-01', 42000.00),
  ('APAC',  DATE '2023-02-01', 38500.00),
  ('APAC',  DATE '2023-03-01', 51200.00),
  ('APAC',  DATE '2023-04-01', 47800.00),
  ('APAC',  DATE '2023-05-01', 44100.00),
  ('APAC',  DATE '2023-06-01', 49500.00),
  ('APAC',  DATE '2024-01-01', 55000.00),
  ('APAC',  DATE '2024-02-01', 43200.00),
  ('APAC',  DATE '2024-03-01', 61500.00),
  ('APAC',  DATE '2024-04-01', 52900.00),
  ('APAC',  DATE '2024-05-01', 48700.00),
  ('APAC',  DATE '2024-06-01', 57200.00),
  ('EMEA',  DATE '2023-01-01', 31000.00),
  ('EMEA',  DATE '2023-02-01', 29500.00),
  ('EMEA',  DATE '2023-03-01', 35000.00),
  ('EMEA',  DATE '2023-04-01', 33000.00),
  ('EMEA',  DATE '2023-05-01', 30500.00),
  ('EMEA',  DATE '2023-06-01', 34200.00),
  ('EMEA',  DATE '2024-01-01', 38500.00),
  ('EMEA',  DATE '2024-02-01', 31000.00),
  ('EMEA',  DATE '2024-03-01', 40200.00),
  ('EMEA',  DATE '2024-04-01', 36800.00),
  ('EMEA',  DATE '2024-05-01', 33900.00),
  ('EMEA',  DATE '2024-06-01', 39100.00)
AS t(region, month, revenue);
```

### Dataset 2: Daily e-commerce orders

```sql
CREATE OR REPLACE TEMP VIEW daily_orders AS
SELECT * FROM VALUES
  -- Week 1 (Mon-Sun): 2024-07-01 to 2024-07-07
  (DATE '2024-07-01', 'electronics', 145, 28500.00),
  (DATE '2024-07-02', 'electronics', 132, 25800.00),
  (DATE '2024-07-03', 'clothing',     98, 12400.00),
  (DATE '2024-07-04', 'electronics', 110, 21500.00),
  (DATE '2024-07-05', 'clothing',    115, 14200.00),
  (DATE '2024-07-06', 'electronics', 165, 32000.00),
  (DATE '2024-07-07', 'clothing',    140, 17500.00),
  -- Week 2 (Mon-Sun): 2024-07-08 to 2024-07-14
  (DATE '2024-07-08', 'electronics', 155, 30100.00),
  (DATE '2024-07-09', 'electronics', 148, 29000.00),
  (DATE '2024-07-10', 'clothing',    105, 13100.00),
  (DATE '2024-07-11', 'electronics', 120, 23400.00),
  (DATE '2024-07-12', 'clothing',    128, 15900.00),
  (DATE '2024-07-13', 'electronics', 180, 35200.00),
  (DATE '2024-07-14', 'clothing',    152, 19000.00),
  -- Week 3 (Mon-Sun): 2024-07-15 to 2024-07-21
  (DATE '2024-07-15', 'electronics', 160, 31200.00),
  (DATE '2024-07-16', 'electronics', 142, 27800.00),
  (DATE '2024-07-17', 'clothing',    112, 14000.00),
  (DATE '2024-07-18', 'electronics', 135, 26400.00),
  (DATE '2024-07-19', 'clothing',    138, 17100.00),
  (DATE '2024-07-20', 'electronics', 190, 37100.00),
  (DATE '2024-07-21', 'clothing',    158, 19700.00)
AS t(order_date, category, order_count, revenue);
```

### Dataset 3: Quarterly SaaS metrics

```sql
CREATE OR REPLACE TEMP VIEW quarterly_metrics AS
SELECT * FROM VALUES
  (2023, 1, 2500,  185000.00, 92.1),
  (2023, 2, 2800,  210000.00, 91.5),
  (2023, 3, 3100,  238000.00, 90.8),
  (2023, 4, 3400,  265000.00, 91.2),
  (2024, 1, 3200,  248000.00, 93.0),
  (2024, 2, 3600,  280000.00, 92.8),
  (2024, 3, 3900,  305000.00, 93.5),
  (2024, 4, 4200,  335000.00, 94.1)
AS t(yr, quarter, active_users, mrr, retention_pct);
```

---

## :material-flask-outline: Pattern 1 — Year-over-Year with LAG

`LAG` with an offset equal to the number of periods per year retrieves the same period from the previous year.

```sql
WITH ordered AS (
    SELECT
        region,
        month,
        revenue,
        LAG(revenue, 6) OVER (
            PARTITION BY region ORDER BY month
        ) AS prev_year_revenue
    FROM monthly_revenue
)
SELECT
    region,
    month,
    revenue,
    prev_year_revenue,
    ROUND((revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0) * 100, 1) AS yoy_pct
FROM ordered
WHERE prev_year_revenue IS NOT NULL
ORDER BY region, month;
```

??? success "Expected output"

    | region | month      | revenue  | prev_year_revenue | yoy_pct |
    |--------|------------|----------|-------------------|---------|
    | APAC   | 2024-01-01 | 55000.00 | 42000.00          | 31.0    |
    | APAC   | 2024-02-01 | 43200.00 | 38500.00          | 12.2    |
    | APAC   | 2024-03-01 | 61500.00 | 51200.00          | 20.1    |
    | APAC   | 2024-04-01 | 52900.00 | 47800.00          | 10.7    |
    | APAC   | 2024-05-01 | 48700.00 | 44100.00          | 10.4    |
    | APAC   | 2024-06-01 | 57200.00 | 49500.00          | 15.6    |
    | EMEA   | 2024-01-01 | 38500.00 | 31000.00          | 24.2    |
    | EMEA   | 2024-02-01 | 31000.00 | 29500.00          | 5.1     |
    | EMEA   | 2024-03-01 | 40200.00 | 35000.00          | 14.9    |
    | EMEA   | 2024-04-01 | 36800.00 | 33000.00          | 11.5    |
    | EMEA   | 2024-05-01 | 33900.00 | 30500.00          | 11.1    |
    | EMEA   | 2024-06-01 | 39100.00 | 34200.00          | 14.3    |

---

## :material-flask-outline: Pattern 2 — YoY via Self-Join (robust for sparse data)

Self-join on matching `MONTH` and `YEAR - 1` handles sparse datasets where `LAG` offsets are unreliable.

```sql
SELECT
    cur.region,
    cur.month                                                    AS current_month,
    cur.revenue                                                  AS current_revenue,
    py.revenue                                                   AS prior_year_revenue,
    ROUND((cur.revenue - py.revenue) / NULLIF(py.revenue, 0) * 100, 1) AS yoy_pct,
    ROUND(cur.revenue - py.revenue, 2)                           AS yoy_delta
FROM monthly_revenue AS cur
JOIN monthly_revenue AS py
    ON  cur.region       = py.region
    AND MONTH(cur.month) = MONTH(py.month)
    AND YEAR(cur.month)  = YEAR(py.month) + 1
ORDER BY cur.region, cur.month;
```

??? success "Expected output (first 6 rows)"

    | region | current_month | current_revenue | prior_year_revenue | yoy_pct | yoy_delta |
    |--------|---------------|-----------------|-------------------|---------|-----------|
    | APAC   | 2024-01-01    | 55000.00        | 42000.00          | 31.0    | 13000.00  |
    | APAC   | 2024-02-01    | 43200.00        | 38500.00          | 12.2    | 4700.00   |
    | APAC   | 2024-03-01    | 61500.00        | 51200.00          | 20.1    | 10300.00  |
    | APAC   | 2024-04-01    | 52900.00        | 47800.00          | 10.7    | 5100.00   |
    | APAC   | 2024-05-01    | 48700.00        | 44100.00          | 10.4    | 4600.00   |
    | APAC   | 2024-06-01    | 57200.00        | 49500.00          | 15.6    | 7700.00   |

---

## :material-flask-outline: Pattern 3 — Month-over-Month with LAG(1)

```sql
SELECT
    region,
    month,
    revenue,
    LAG(revenue, 1) OVER (PARTITION BY region, YEAR(month) ORDER BY month) AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue, 1) OVER (PARTITION BY region, YEAR(month) ORDER BY month))
        / NULLIF(LAG(revenue, 1) OVER (PARTITION BY region, YEAR(month) ORDER BY month), 0) * 100,
    1) AS mom_pct
FROM monthly_revenue
WHERE YEAR(month) = 2024
ORDER BY region, month;
```

??? success "Expected output"

    | region | month      | revenue  | prev_month_revenue | mom_pct |
    |--------|------------|----------|--------------------|---------|
    | APAC   | 2024-01-01 | 55000.00 | NULL               | NULL    |
    | APAC   | 2024-02-01 | 43200.00 | 55000.00           | -21.5   |
    | APAC   | 2024-03-01 | 61500.00 | 43200.00           | 42.4    |
    | APAC   | 2024-04-01 | 52900.00 | 61500.00           | -14.0   |
    | APAC   | 2024-05-01 | 48700.00 | 52900.00           | -7.9    |
    | APAC   | 2024-06-01 | 57200.00 | 48700.00           | 17.5    |
    | EMEA   | 2024-01-01 | 38500.00 | NULL               | NULL    |
    | EMEA   | 2024-02-01 | 31000.00 | 38500.00           | -19.5   |
    | EMEA   | 2024-03-01 | 40200.00 | 31000.00           | 29.7    |
    | EMEA   | 2024-04-01 | 36800.00 | 40200.00           | -8.5    |
    | EMEA   | 2024-05-01 | 33900.00 | 36800.00           | -7.9    |
    | EMEA   | 2024-06-01 | 39100.00 | 33900.00           | 15.3    |

!!! tip "Avoid cross-year MoM"
    Partition by `YEAR(month)` to prevent Jan 2024 comparing against Dec 2023's distant month (Apr in sparse data). Without it, `LAG(1)` blindly takes the preceding row.

---

## :material-flask-outline: Pattern 4 — Running YTD vs Prior Year YTD

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
    ROUND((cur.ytd_revenue - py.ytd_revenue) / NULLIF(py.ytd_revenue, 0) * 100, 1) AS ytd_yoy_pct
FROM ytd AS cur
JOIN ytd AS py
    ON cur.region = py.region
   AND cur.mo     = py.mo
   AND cur.yr     = py.yr + 1
ORDER BY cur.region, cur.mo;
```

??? success "Expected output"

    | region | month_num | current_year | ytd_revenue | prior_ytd_revenue | ytd_yoy_pct |
    |--------|-----------|--------------|-------------|-------------------|-------------|
    | APAC   | 1         | 2024         | 55000.00    | 42000.00          | 31.0        |
    | APAC   | 2         | 2024         | 98200.00    | 80500.00          | 22.0        |
    | APAC   | 3         | 2024         | 159700.00   | 131700.00         | 21.3        |
    | APAC   | 4         | 2024         | 212600.00   | 179500.00         | 18.4        |
    | APAC   | 5         | 2024         | 261300.00   | 223600.00         | 16.9        |
    | APAC   | 6         | 2024         | 318500.00   | 273100.00         | 16.6        |
    | EMEA   | 1         | 2024         | 38500.00    | 31000.00          | 24.2        |
    | EMEA   | 2         | 2024         | 69500.00    | 60500.00          | 14.9        |
    | EMEA   | 3         | 2024         | 109700.00   | 95500.00          | 14.9        |
    | EMEA   | 4         | 2024         | 146500.00   | 128500.00         | 14.0        |
    | EMEA   | 5         | 2024         | 180400.00   | 159000.00         | 13.5        |
    | EMEA   | 6         | 2024         | 219500.00   | 193200.00         | 13.6        |

---

## :material-flask-outline: Pattern 5 — Week-over-Week Comparison

```sql
WITH weekly AS (
    SELECT
        DATE_TRUNC('week', order_date)  AS week_start,
        category,
        SUM(order_count)                AS total_orders,
        ROUND(SUM(revenue), 2)          AS total_revenue
    FROM daily_orders
    GROUP BY DATE_TRUNC('week', order_date), category
)
SELECT
    week_start,
    category,
    total_orders,
    total_revenue,
    LAG(total_orders) OVER (PARTITION BY category ORDER BY week_start)  AS prev_week_orders,
    LAG(total_revenue) OVER (PARTITION BY category ORDER BY week_start) AS prev_week_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY category ORDER BY week_start))
        / NULLIF(LAG(total_revenue) OVER (PARTITION BY category ORDER BY week_start), 0) * 100, 1
    ) AS wow_revenue_pct
FROM weekly
ORDER BY category, week_start;
```

??? success "Expected output"

    | week_start | category    | total_orders | total_revenue | prev_week_orders | prev_week_revenue | wow_revenue_pct |
    |------------|-------------|--------------|---------------|------------------|-------------------|-----------------|
    | 2024-07-01 | clothing    | 353          | 44100.00      | NULL             | NULL              | NULL            |
    | 2024-07-08 | clothing    | 385          | 48000.00      | 353              | 44100.00          | 8.8             |
    | 2024-07-15 | clothing    | 408          | 50800.00      | 385              | 48000.00          | 5.8             |
    | 2024-07-01 | electronics | 552          | 107800.00     | NULL             | NULL              | NULL            |
    | 2024-07-08 | electronics | 603          | 117700.00     | 552              | 107800.00         | 9.2             |
    | 2024-07-15 | electronics | 627          | 122500.00     | 603              | 117700.00         | 4.1             |

---

## :material-flask-outline: Pattern 6 — Quarter-over-Quarter with Growth Indicators

```sql
SELECT
    yr,
    quarter,
    active_users,
    mrr,
    retention_pct,
    LAG(mrr) OVER (ORDER BY yr, quarter)                     AS prev_qtr_mrr,
    ROUND((mrr - LAG(mrr) OVER (ORDER BY yr, quarter))
        / NULLIF(LAG(mrr) OVER (ORDER BY yr, quarter), 0) * 100, 1) AS qoq_mrr_pct,
    LAG(active_users) OVER (ORDER BY yr, quarter)            AS prev_qtr_users,
    active_users - LAG(active_users) OVER (ORDER BY yr, quarter) AS user_growth
FROM quarterly_metrics
ORDER BY yr, quarter;
```

??? success "Expected output"

    | yr   | quarter | active_users | mrr       | retention_pct | prev_qtr_mrr | qoq_mrr_pct | prev_qtr_users | user_growth |
    |------|---------|--------------|-----------|---------------|--------------|-------------|----------------|-------------|
    | 2023 | 1       | 2500         | 185000.00 | 92.1          | NULL         | NULL        | NULL           | NULL        |
    | 2023 | 2       | 2800         | 210000.00 | 91.5          | 185000.00    | 13.5        | 2500           | 300         |
    | 2023 | 3       | 3100         | 238000.00 | 90.8          | 210000.00    | 13.3        | 2800           | 300         |
    | 2023 | 4       | 3400         | 265000.00 | 91.2          | 238000.00    | 11.3        | 3100           | 300         |
    | 2024 | 1       | 3200         | 248000.00 | 93.0          | 265000.00    | -6.4        | 3400           | -200        |
    | 2024 | 2       | 3600         | 280000.00 | 92.8          | 248000.00    | 12.9        | 3200           | 400         |
    | 2024 | 3       | 3900         | 305000.00 | 93.5          | 280000.00    | 8.9         | 3600           | 300         |
    | 2024 | 4       | 4200         | 335000.00 | 94.1          | 305000.00    | 9.8         | 3900           | 300         |

---

## :material-flask-outline: Pattern 7 — Same-Day-of-Week Comparison (daily)

Compare each day against the same weekday in the prior week:

```sql
SELECT
    order_date,
    category,
    order_count,
    revenue,
    LAG(order_count, 7) OVER (PARTITION BY category ORDER BY order_date) AS same_day_prev_week_orders,
    LAG(revenue, 7) OVER (PARTITION BY category ORDER BY order_date)     AS same_day_prev_week_rev,
    ROUND(
        (revenue - LAG(revenue, 7) OVER (PARTITION BY category ORDER BY order_date))
        / NULLIF(LAG(revenue, 7) OVER (PARTITION BY category ORDER BY order_date), 0) * 100, 1
    ) AS wow_pct
FROM daily_orders
WHERE category = 'electronics'
ORDER BY order_date;
```

??? success "Expected output"

    | order_date | category    | order_count | revenue  | same_day_prev_week_orders | same_day_prev_week_rev | wow_pct |
    |------------|-------------|-------------|----------|--------------------------|------------------------|---------|
    | 2024-07-01 | electronics | 145         | 28500.00 | NULL                     | NULL                   | NULL    |
    | 2024-07-02 | electronics | 132         | 25800.00 | NULL                     | NULL                   | NULL    |
    | ...        | ...         | ...         | ...      | ...                      | ...                    | ...     |
    | 2024-07-08 | electronics | 155         | 30100.00 | 145                      | 28500.00               | 5.6     |
    | 2024-07-09 | electronics | 148         | 29000.00 | 132                      | 25800.00               | 12.4    |
    | 2024-07-11 | electronics | 120         | 23400.00 | 110                      | 21500.00               | 8.8     |
    | 2024-07-13 | electronics | 180         | 35200.00 | 165                      | 32000.00               | 10.0    |
    | 2024-07-15 | electronics | 160         | 31200.00 | 155                      | 30100.00               | 3.7     |
    | 2024-07-16 | electronics | 142         | 27800.00 | 148                      | 29000.00               | -4.1    |

---

## :material-flask-outline: Pattern 8 — Period Comparison with FILTER (Pivot Style)

Compare multiple years side-by-side in a single row using `FILTER`:

```sql
SELECT
    MONTH(month) AS month_num,
    SUM(revenue) FILTER (WHERE YEAR(month) = 2023 AND region = 'APAC') AS apac_2023,
    SUM(revenue) FILTER (WHERE YEAR(month) = 2024 AND region = 'APAC') AS apac_2024,
    SUM(revenue) FILTER (WHERE YEAR(month) = 2023 AND region = 'EMEA') AS emea_2023,
    SUM(revenue) FILTER (WHERE YEAR(month) = 2024 AND region = 'EMEA') AS emea_2024
FROM monthly_revenue
GROUP BY MONTH(month)
ORDER BY month_num;
```

??? success "Expected output"

    | month_num | apac_2023 | apac_2024 | emea_2023 | emea_2024 |
    |-----------|-----------|-----------|-----------|-----------|
    | 1         | 42000.00  | 55000.00  | 31000.00  | 38500.00  |
    | 2         | 38500.00  | 43200.00  | 29500.00  | 31000.00  |
    | 3         | 51200.00  | 61500.00  | 35000.00  | 40200.00  |
    | 4         | 47800.00  | 52900.00  | 33000.00  | 36800.00  |
    | 5         | 44100.00  | 48700.00  | 30500.00  | 33900.00  |
    | 6         | 49500.00  | 57200.00  | 34200.00  | 39100.00  |

---

## :material-animation-play: Interactive Demo

> Click **APAC** or **EMEA** to switch regions.
> Hover the large solid dots (2024) or small dashed dots (2023) to see the YoY change.

<div id="viz-period-compare" class="ts-viz"></div>

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern | Key |
|----------|---------|-----|
| Same-month YoY (dense data) | `LAG(n)` | n = periods per year in the partition |
| Same-month YoY (sparse data) | Self-join on `MONTH()` + `YEAR() - 1` | Handles missing months gracefully |
| Month-over-Month | `LAG(1) PARTITION BY YEAR` | Avoids cross-year comparison |
| Running YTD vs prior YTD | `SUM OVER ()` + self-join | Cumulative comparison |
| Week-over-Week | Aggregate to weeks, then `LAG(1)` | `DATE_TRUNC('week', date)` |
| Same weekday comparison | `LAG(7)` on daily data | Compare Mon vs Mon |
| Quarter-over-Quarter | `LAG(1) ORDER BY yr, qtr` | SaaS / financial metrics |
| Multi-year pivot | `FILTER (WHERE YEAR = ...)` | Side-by-side columns |
| NULL-safe division | `NULLIF(denominator, 0)` | Prevents division by zero |

---

## :material-magnify: Behavior Notes

1. `LAG(n)` requires **dense, contiguous data** — if months are missing, the offset won't match the intended prior period. Use self-joins for sparse data.
2. Always wrap the denominator in `NULLIF(prev_value, 0)` to avoid division-by-zero errors.
3. `PARTITION BY region, YEAR(month)` prevents MoM from crossing year boundaries unintentionally.
4. For YTD comparisons, ensure both years have the same number of months — otherwise the cumulative sum is not comparable.
5. `FILTER (WHERE ...)` inside aggregates is the cleanest way to build pivot-style period comparisons without `CASE WHEN`.
