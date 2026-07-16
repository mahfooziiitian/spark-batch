# :material-calendar-sync: Seasonality Detection

Compare values across **week-over-week**, **month-over-month**, and **year-over-year**
periods to identify recurring patterns, seasonal peaks, and cyclical trends.

---

## :material-sitemap: Detection Flow

```mermaid
flowchart LR
    RAW[Time Series Data] --> ALIGN[Period Alignment\nDATE_TRUNC · EXTRACT]
    ALIGN --> COMPARE[Period Comparison\nLAG by period offset]
    COMPARE --> RATIO[Growth Ratios\nWoW · MoM · YoY]
    RATIO --> PATTERN[Seasonal Pattern\nRecurring peaks & troughs]

    style RAW fill:#e3f2fd,stroke:#1e88e5
    style ALIGN fill:#e8f5e9,stroke:#43a047
    style COMPARE fill:#fff3e0,stroke:#fb8c00
    style PATTERN fill:#fce4ec,stroke:#e53935
```

---

## :material-code-tags: Syntax

### Sample data

```sql
CREATE OR REPLACE TEMP VIEW weekly_revenue AS
SELECT * FROM VALUES
  ('electronics', DATE '2023-01-02', 12000),
  ('electronics', DATE '2023-01-09', 11500),
  ('electronics', DATE '2023-01-16', 13200),
  ('electronics', DATE '2023-01-23', 12800),
  ('electronics', DATE '2023-02-06', 14500),
  ('electronics', DATE '2023-02-13', 13900),
  ('electronics', DATE '2023-03-06', 15200),
  ('electronics', DATE '2023-06-05', 18500),
  ('electronics', DATE '2023-09-04', 16800),
  ('electronics', DATE '2023-12-04', 22000),
  ('electronics', DATE '2024-01-01', 14200),
  ('electronics', DATE '2024-01-08', 13800),
  ('electronics', DATE '2024-01-15', 15500),
  ('electronics', DATE '2024-01-22', 14900),
  ('electronics', DATE '2024-02-05', 16800),
  ('electronics', DATE '2024-02-12', 16200),
  ('electronics', DATE '2024-03-04', 17500),
  ('electronics', DATE '2024-06-03', 21200),
  ('electronics', DATE '2024-09-02', 19500),
  ('electronics', DATE '2024-12-02', 25800),
  ('clothing',    DATE '2023-01-02', 8000),
  ('clothing',    DATE '2023-06-05', 12500),
  ('clothing',    DATE '2023-12-04', 15000),
  ('clothing',    DATE '2024-01-01', 9200),
  ('clothing',    DATE '2024-06-03', 14800),
  ('clothing',    DATE '2024-12-02', 17500)
AS t(category, week_start, revenue);
```

---

### Week-over-week (WoW) comparison

Compare each week to the immediately preceding week.

```sql
SELECT
    category,
    week_start,
    revenue,
    LAG(revenue, 1) OVER (
        PARTITION BY category ORDER BY week_start
    )                                              AS prev_week_revenue,
    revenue - LAG(revenue, 1) OVER (
        PARTITION BY category ORDER BY week_start
    )                                              AS wow_change,
    ROUND(
        (revenue - LAG(revenue, 1) OVER (
            PARTITION BY category ORDER BY week_start
        )) * 100.0
        / NULLIF(LAG(revenue, 1) OVER (
            PARTITION BY category ORDER BY week_start
        ), 0),
        2
    )                                              AS wow_pct
FROM weekly_revenue
ORDER BY category, week_start;
-- Result (electronics, partial):
-- |week_start |revenue|prev_week|wow_change|wow_pct|
-- |2024-01-01 |14200  |22000    |-7800     |-35.45 |
-- |2024-01-08 |13800  |14200    |-400      |-2.82  |
-- |2024-01-15 |15500  |13800    |1700      |12.32  |
```

---

### Month-over-month (MoM) comparison

Aggregate to monthly granularity and compare consecutive months.

```sql
WITH monthly AS (
    SELECT
        category,
        DATE_TRUNC('month', week_start)            AS month,
        SUM(revenue)                               AS monthly_revenue
    FROM weekly_revenue
    GROUP BY category, DATE_TRUNC('month', week_start)
)
SELECT
    category,
    month,
    monthly_revenue,
    LAG(monthly_revenue, 1) OVER (
        PARTITION BY category ORDER BY month
    )                                              AS prev_month_revenue,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue, 1) OVER (
            PARTITION BY category ORDER BY month
        )) * 100.0
        / NULLIF(LAG(monthly_revenue, 1) OVER (
            PARTITION BY category ORDER BY month
        ), 0),
        2
    )                                              AS mom_pct
FROM monthly
ORDER BY category, month;
```

---

### Year-over-year (YoY) comparison

Compare each period to the **same period last year** to isolate seasonal effects.

```sql
WITH monthly AS (
    SELECT
        category,
        DATE_TRUNC('month', week_start)            AS month,
        YEAR(week_start)                           AS yr,
        MONTH(week_start)                          AS mo,
        SUM(revenue)                               AS monthly_revenue
    FROM weekly_revenue
    GROUP BY
        category,
        DATE_TRUNC('month', week_start),
        YEAR(week_start),
        MONTH(week_start)
)
SELECT
    curr.category,
    curr.month,
    curr.monthly_revenue                           AS current_revenue,
    prev.monthly_revenue                           AS prior_year_revenue,
    curr.monthly_revenue - prev.monthly_revenue    AS yoy_change,
    ROUND(
        (curr.monthly_revenue - prev.monthly_revenue) * 100.0
        / NULLIF(prev.monthly_revenue, 0),
        2
    )                                              AS yoy_pct
FROM monthly curr
LEFT JOIN monthly prev
    ON curr.category = prev.category
    AND curr.yr = prev.yr + 1
    AND curr.mo = prev.mo
ORDER BY curr.category, curr.month;
-- Result (electronics):
-- |month     |current_revenue|prior_year|yoy_change|yoy_pct|
-- |2024-01   |58400          |49500     |8900      |17.98  |
-- |2024-06   |21200          |18500     |2700      |14.59  |
-- |2024-12   |25800          |22000     |3800      |17.27  |
```

---

### Seasonal index calculation

Compute a seasonal index — how much each month deviates from the annual average.

```sql
WITH monthly AS (
    SELECT
        category,
        YEAR(week_start)                           AS yr,
        MONTH(week_start)                          AS mo,
        SUM(revenue)                               AS monthly_revenue
    FROM weekly_revenue
    GROUP BY category, YEAR(week_start), MONTH(week_start)
),
annual_avg AS (
    SELECT
        category,
        yr,
        AVG(monthly_revenue)                       AS avg_monthly
    FROM monthly
    GROUP BY category, yr
)
SELECT
    m.category,
    m.yr,
    m.mo,
    m.monthly_revenue,
    a.avg_monthly,
    ROUND(m.monthly_revenue / a.avg_monthly, 3)    AS seasonal_index
FROM monthly m
JOIN annual_avg a
    ON m.category = a.category
    AND m.yr = a.yr
ORDER BY m.category, m.yr, m.mo;
-- seasonal_index > 1.0 = above-average month (peak season)
-- seasonal_index < 1.0 = below-average month (off season)
```

---

### Multi-period trend summary

Combine WoW, MoM, and YoY into a single dashboard view.

```sql
WITH weekly_metrics AS (
    SELECT
        category,
        week_start,
        revenue,
        LAG(revenue, 1) OVER w                     AS prev_week,
        LAG(revenue, 4) OVER w                     AS four_weeks_ago,
        LAG(revenue, 52) OVER w                    AS same_week_last_year
    FROM weekly_revenue
    WINDOW w AS (PARTITION BY category ORDER BY week_start)
)
SELECT
    category,
    week_start,
    revenue,
    -- WoW
    ROUND(
        (revenue - prev_week) * 100.0
        / NULLIF(prev_week, 0), 2
    )                                              AS wow_pct,
    -- 4-week (approx MoM)
    ROUND(
        (revenue - four_weeks_ago) * 100.0
        / NULLIF(four_weeks_ago, 0), 2
    )                                              AS mom_approx_pct,
    -- YoY
    ROUND(
        (revenue - same_week_last_year) * 100.0
        / NULLIF(same_week_last_year, 0), 2
    )                                              AS yoy_pct
FROM weekly_metrics
WHERE prev_week IS NOT NULL
ORDER BY category, week_start;
```

---

## :material-information-outline: Key Concepts

| Comparison | LAG Offset | Best For |
|-----------|------------|----------|
| **Week-over-week** | `LAG(col, 1)` on weekly data | Short-term momentum |
| **Month-over-month** | `LAG(col, 1)` on monthly aggregates | Medium-term trend |
| **Year-over-year** | Self-join on `yr + 1, mo` | Seasonal isolation |
| **Seasonal index** | `value / annual_avg` | Quantify seasonal strength |

!!! tip "YoY removes seasonality"
    WoW and MoM conflate growth with seasonality. YoY comparison neutralises
    seasonal effects — a December spike looks normal when compared to last December.

!!! note "Sparse data handling"
    If weeks/months are missing, use gap-filling before LAG — otherwise `LAG(1)`
    may reference a non-adjacent period. See [Gap Filling](gap_filling.md).

---

## :material-lightbulb-outline: When to Use

| Scenario | Period |
|----------|--------|
| Weekly sales reporting | WoW — detect immediate shifts |
| Monthly business review | MoM — track operational momentum |
| Annual planning / budgets | YoY — isolate real growth from seasonality |
| Inventory pre-positioning | Seasonal index — predict demand peaks |
| Marketing calendar | Combine all — find optimal campaign windows |

---

## :material-speedometer: Performance Notes

| Tip | Reason |
|-----|--------|
| Pre-aggregate to target granularity first | Reduces rows before window functions |
| Use `DATE_TRUNC` for period alignment | Deterministic grouping, partition-friendly |
| Self-join for YoY is clearer than `LAG(12)` | Works correctly even with missing months |
| Named `WINDOW` for multiple LAG offsets | Single sort pass shared across features |
| Filter to relevant years only | Avoids computing over full history |

---

## :material-arrow-right: Related

- [Period Comparison](../../sequence/period_comparison.md) — generalised LAG/LEAD comparisons
- [Forecast Features](forecast_features.md) — lag + rolling stats for ML
- [Time Aggregation](time_aggregation.md) — DATE_TRUNC-based grouping
- [Gap Filling](gap_filling.md) — handle missing periods before comparison
