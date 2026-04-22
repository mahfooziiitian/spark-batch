# :material-arrow-left-right: LAG & LEAD

`LAG` and `LEAD` are SQL window functions that access values from **other rows** within an ordered window frame — without a self-join.

- `LAG(col, n)` — value from **n rows before** the current row
- `LEAD(col, n)` — value from **n rows after** the current row
- Both accept a third argument as the **default** when the referenced row does not exist

---

## :material-pin: Syntax

```sql
LAG(expression, offset, default)  OVER (PARTITION BY ... ORDER BY ...)
LEAD(expression, offset, default) OVER (PARTITION BY ... ORDER BY ...)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `expression` | Any | Column or expression to access |
| `offset` | Integer | Number of rows to look back (LAG) or forward (LEAD). Default: 1 |
| `default` | Any | Value returned when the referenced row is outside the partition |

---

## :material-calendar-arrow-left: Day-over-Day Comparison

```sql
SELECT
    sale_date,
    region,
    revenue,
    LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date)  AS prev_day,
    revenue - LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS dod_delta,
    ROUND(
        (revenue - LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date))
        / NULLIF(LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date), 0) * 100,
        2
    ) AS dod_pct
FROM daily_metrics
ORDER BY region, sale_date;
```

---

## :material-calendar-arrow-right: Look Ahead with LEAD

```sql
SELECT
    sale_date,
    region,
    revenue,
    LEAD(revenue, 1, revenue) OVER (PARTITION BY region ORDER BY sale_date) AS next_day_revenue
FROM daily_metrics
ORDER BY region, sale_date;
-- Third argument (revenue) is returned instead of NULL on the last row
```

---

## :material-swap-horizontal: State Transition Detection

Detect the exact moment a value changes — useful for device status, order state machines, and alert systems.

```sql
SELECT
    device_id,
    event_time,
    status,
    LAG(status) OVER (PARTITION BY device_id ORDER BY event_time) AS prev_status,
    status <> LAG(status) OVER (PARTITION BY device_id ORDER BY event_time) AS is_transition
FROM device_status
ORDER BY device_id, event_time;
```

---

## :material-trending-down: Consecutive Decline Flag

```sql
WITH with_lag AS (
    SELECT
        sale_date,
        region,
        revenue,
        LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date) AS prev_revenue
    FROM daily_metrics
)

SELECT *, revenue < prev_revenue AS is_decline
FROM with_lag
WHERE prev_revenue IS NOT NULL
ORDER BY region, sale_date;
```

---

## :material-trending-up: Growth Streak Counter

```sql
WITH flagged AS (
    SELECT
        sale_date,
        region,
        revenue,
        CASE
            WHEN revenue > LAG(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date)
            THEN 1 ELSE 0
        END AS is_growth
    FROM daily_metrics
),
numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date) AS rn,
        SUM(is_growth) OVER (PARTITION BY region ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS growth_count
    FROM flagged
)
SELECT sale_date, region, revenue,
    SUM(is_growth) OVER (
        PARTITION BY region, (rn - growth_count)
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS growth_streak
FROM numbered
ORDER BY region, sale_date;
```

---

## :material-function-variant: Lead-Lag Spread (Central Difference)

A symmetric momentum signal: `(next - prev) / 2`.

```sql
SELECT
    sale_date,
    region,
    revenue,
    ROUND(
        (LEAD(revenue,1) OVER (PARTITION BY region ORDER BY sale_date)
         - LAG(revenue,1)  OVER (PARTITION BY region ORDER BY sale_date)) / 2.0,
        2
    ) AS central_diff
FROM daily_metrics
ORDER BY region, sale_date;
```

---

## :material-table-check: When to Use

| Scenario | Function |
|----------|----------|
| Period-over-period % change | `LAG(col, 1)` |
| Forecasting next value | `LEAD(col, 1)` |
| State-change detection | `LAG(status)` |
| Streak counting | `LAG` + cumulative `SUM` |
| Time between events | `LAG(event_time)` |
| Momentum / rate-of-change | `LAG(col, N)` |

!!! tip
    Always use `NULLIF(lag_value, 0)` in the denominator of percentage calculations to avoid division-by-zero errors.
