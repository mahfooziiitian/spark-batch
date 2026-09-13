# :material-arrow-left-right: LAG & LEAD

`LAG` and `LEAD` are SQL window functions that access values from **other rows** within an ordered window frame — without a self-join.

- `LAG(col, n)` — value from **n rows before** the current row
- `LEAD(col, n)` — value from **n rows after** the current row
- Both accept a third argument as the **default** when the referenced row does not exist

______________________________________________________________________

## :material-play-circle: Interactive Demo

Click any bar to see its **LAG** (previous day, amber) and **LEAD** (next day, teal) values annotated live.

<div id="viz-laglead" class="ts-viz"></div>

*7 days of daily revenue. Purple = current row. Arrows show the value `LAG` and `LEAD` would return.*

______________________________________________________________________

## :material-pin: Syntax

```sql
LAG(expression, offset, default)  OVER (PARTITION BY ... ORDER BY ...)
LEAD(expression, offset, default) OVER (PARTITION BY ... ORDER BY ...)
```

| Parameter    | Type    | Description                                                                      |
| ------------ | ------- | -------------------------------------------------------------------------------- |
| `expression` | Any     | Column or expression to access                                                   |
| `offset`     | Integer | Number of rows to look back (LAG) or forward (LEAD). Default: `1`                |
| `default`    | Any     | Value returned when the referenced row is outside the partition. Default: `NULL` |

!!! note

    Both functions are evaluated **after** `PARTITION BY` and `ORDER BY` are applied.
    The window frame clause (`ROWS BETWEEN …`) is **ignored** — offset is always row-based.

______________________________________________________________________

## :material-calendar-arrow-left: Day-over-Day Comparison

```sql
SELECT
    sale_date,
    region,
    revenue,
    LAG(revenue, 1)                        OVER w AS prev_day_revenue,
    revenue - LAG(revenue, 1)              OVER w AS dod_delta,
    ROUND(
        (revenue - LAG(revenue, 1) OVER w)
        / NULLIF(LAG(revenue, 1) OVER w, 0) * 100,
        2
    ) AS dod_pct_change
FROM daily_metrics
WINDOW w AS (PARTITION BY region ORDER BY sale_date)
ORDER BY region, sale_date;
```

!!! tip

    Use a named `WINDOW` clause to avoid repeating the `OVER (...)` specification.

______________________________________________________________________

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

______________________________________________________________________

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

______________________________________________________________________

## :material-clock-outline: Time Between Events

Calculate the gap between consecutive events per user.

```sql
SELECT
    user_id,
    event_time,
    event_type,
    ROUND(
        (unix_timestamp(event_time)
         - unix_timestamp(LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time))) / 60.0,
        2
    ) AS minutes_since_last_event
FROM clickstream
ORDER BY user_id, event_time;
```

______________________________________________________________________

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

______________________________________________________________________

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
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sale_date) AS rn,
        SUM(is_growth) OVER (PARTITION BY region ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_growth
    FROM flagged
)
SELECT
    sale_date,
    region,
    revenue,
    SUM(is_growth) OVER (
        PARTITION BY region, (rn - cumulative_growth)
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS growth_streak
FROM numbered
ORDER BY region, sale_date;
```

______________________________________________________________________

## :material-function-variant: Lead-Lag Spread (Central Difference)

A symmetric momentum signal: `(next − prev) / 2`. Useful for smoothing noisy metrics.

```sql
SELECT
    sale_date,
    region,
    revenue,
    ROUND(
        (LEAD(revenue, 1) OVER (PARTITION BY region ORDER BY sale_date)
         - LAG(revenue, 1)  OVER (PARTITION BY region ORDER BY sale_date)) / 2.0,
        2
    ) AS central_diff
FROM daily_metrics
ORDER BY region, sale_date;
```

______________________________________________________________________

## :material-compare: Multi-Period Comparison

Compare current value against both last week (offset=7) and last month (offset=30).

```sql
SELECT
    sale_date,
    region,
    revenue,
    LAG(revenue, 7)  OVER (PARTITION BY region ORDER BY sale_date) AS wow_baseline,
    LAG(revenue, 30) OVER (PARTITION BY region ORDER BY sale_date) AS mom_baseline,
    ROUND((revenue - LAG(revenue, 7)  OVER (PARTITION BY region ORDER BY sale_date))
          / NULLIF(LAG(revenue, 7)  OVER (PARTITION BY region ORDER BY sale_date), 0) * 100, 2) AS wow_pct,
    ROUND((revenue - LAG(revenue, 30) OVER (PARTITION BY region ORDER BY sale_date))
          / NULLIF(LAG(revenue, 30) OVER (PARTITION BY region ORDER BY sale_date), 0) * 100, 2) AS mom_pct
FROM daily_metrics
ORDER BY region, sale_date;
```

______________________________________________________________________

## :material-alert-circle: Common Pitfalls

| Pitfall                      | Problem                                                  | Fix                                                           |
| ---------------------------- | -------------------------------------------------------- | ------------------------------------------------------------- |
| Division by `LAG` value      | Divide-by-zero when prev = 0                             | Wrap denominator in `NULLIF(…, 0)`                            |
| Missing `ORDER BY` in `OVER` | Non-deterministic results                                | Always specify `ORDER BY`                                     |
| `LAG` across partitions      | LAG(col,1) on first row of each partition returns `NULL` | Provide a default value as 3rd arg                            |
| Frame clause confusion       | `ROWS BETWEEN` is ignored by LAG/LEAD                    | Frame clause has no effect; offset is always row-count        |
| Chained LAG                  | `LAG(LAG(col))` is not valid SQL                         | Use a CTE to materialize the first LAG, then apply the second |

______________________________________________________________________

## :material-database-search: [Databricks] Real-World Example — System Tables

!!! note "[Databricks] Unity Catalog system tables"

    `system.query.history` is a built-in Unity Catalog system table (no
    sample data setup needed) that records real query start and end times.
    An account admin must grant `USE CATALOG` on `system`, `USE SCHEMA` on
    `system.query`, and `SELECT` on `system.query.history` before these
    queries will return rows.

### Consecutive query comparison per warehouse

```sql
-- [Databricks] Requires SELECT on system.query.history
SELECT
    compute.warehouse_id AS warehouse_id,
    start_time,
    total_duration_ms,
    LAG(total_duration_ms, 1) OVER w AS prev_duration_ms,
    LEAD(total_duration_ms, 1) OVER w AS next_duration_ms,
    total_duration_ms - LAG(total_duration_ms, 1) OVER w AS delta_vs_prev_ms,
    ROUND(
        (UNIX_TIMESTAMP(start_time) - UNIX_TIMESTAMP(LAG(end_time, 1) OVER w))
        / 60.0,
        2
    ) AS minutes_since_prev_query
FROM system.query.history
WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
  AND compute.warehouse_id IS NOT NULL
WINDOW w AS (PARTITION BY compute.warehouse_id ORDER BY start_time)
ORDER BY warehouse_id, start_time;
-- Result (illustrative):
-- warehouse_id | start_time           | total_duration_ms | prev_duration_ms | next_duration_ms | delta_vs_prev_ms | minutes_since_prev_query
-- ------------ | -------------------- | ----------------- | ---------------- | ---------------- | ---------------- | ------------------------
-- 6f91a...     | 2024-07-02 09:00:14  | 1840              | NULL             | 2310             | NULL             | NULL
-- 6f91a...     | 2024-07-02 09:12:07  | 2310              | 1840             | 1295             | 470              | 4.22
```

!!! tip "Operational sequences are ideal for LAG and LEAD"

    Query history is already ordered event data, so the same row-to-row
    comparison patterns apply without any synthetic setup. Duration deltas,
    time gaps, and state transitions all come from the same window shape.

______________________________________________________________________

## :material-table-check: When to Use

| Scenario                    | Function                       |
| --------------------------- | ------------------------------ |
| Period-over-period % change | `LAG(col, 1)`                  |
| Forecasting next value      | `LEAD(col, 1)`                 |
| State-change detection      | `LAG(status)`                  |
| Streak counting             | `LAG` + cumulative `SUM`       |
| Time between events         | `LAG(event_time)`              |
| Momentum / rate-of-change   | `LAG(col, N)`                  |
| Multi-period comparison     | `LAG(col, 7)` + `LAG(col, 30)` |

!!! tip

    Always use `NULLIF(lag_value, 0)` in the denominator of percentage calculations to avoid division-by-zero errors.
