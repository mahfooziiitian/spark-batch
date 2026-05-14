# :material-clock-outline: Time-Based Aggregation

Group and aggregate time series data by calendar periods using `DATE_TRUNC`, `TO_DATE`, `YEAR`/`MONTH`/`WEEK`, and time-bucket functions.

---

## :material-sitemap: Overview

```mermaid
graph LR
    A[Timestamp column] --> B["DATE_TRUNC / TO_DATE"]
    B --> C["GROUP BY period"]
    C --> D["Aggregate: COUNT / SUM / AVG"]
    D --> E[Period report]
```

---

## :material-table: Date Truncation Functions

| Function | Truncates to | Example output |
|----------|-------------|----------------|
| `TO_DATE(ts)` | Day | `2025-07-20` |
| `DATE_TRUNC('hour', ts)` | Hour | `2025-07-20 10:00:00` |
| `DATE_TRUNC('day', ts)` | Day | `2025-07-20 00:00:00` |
| `DATE_TRUNC('week', ts)` | Monday of the week | `2025-07-14 00:00:00` |
| `DATE_TRUNC('month', ts)` | First of the month | `2025-07-01 00:00:00` |
| `DATE_TRUNC('quarter', ts)` | First of the quarter | `2025-07-01 00:00:00` |
| `DATE_TRUNC('year', ts)` | First of the year | `2025-01-01 00:00:00` |
| `YEAR(ts)` | Integer year | `2025` |
| `MONTH(ts)` | Integer month | `7` |
| `WEEKOFYEAR(ts)` | ISO week number | `29` |

---

## :material-flask-outline: Examples

### Sample data

```sql
CREATE OR REPLACE TEMP VIEW events AS
SELECT * FROM VALUES
  (1001, 1, TIMESTAMP('2025-07-20 10:00:00'), 'click',    50),
  (1002, 2, TIMESTAMP('2025-07-20 11:05:00'), 'view',     30),
  (1003, 1, TIMESTAMP('2025-07-20 10:10:00'), 'click',    70),
  (1004, 3, TIMESTAMP('2025-07-20 11:15:00'), 'purchase', 90),
  (1005, 2, TIMESTAMP('2025-07-20 10:20:00'), 'click',    20)
AS events(event_id, user_id, event_time, event_type, value);
```

### Hourly aggregation

```sql
SELECT
    DATE_TRUNC('hour', event_time) AS hour_bucket,
    COUNT(*)                       AS event_count,
    SUM(value)                     AS total_value
FROM events
GROUP BY DATE_TRUNC('hour', event_time)
ORDER BY hour_bucket;
```

### Daily total per user

```sql
SELECT
    user_id,
    TO_DATE(event_time)  AS event_date,
    COUNT(*)             AS daily_count,
    SUM(value)           AS daily_value
FROM events
GROUP BY user_id, TO_DATE(event_time)
ORDER BY user_id, event_date;
```

### Weekly aggregation

```sql
SELECT
    DATE_TRUNC('week', event_time) AS week_start,
    WEEKOFYEAR(event_time)         AS iso_week,
    COUNT(*)                       AS event_count,
    SUM(value)                     AS weekly_value
FROM events
GROUP BY DATE_TRUNC('week', event_time), WEEKOFYEAR(event_time)
ORDER BY week_start;
```

### Monthly aggregation

```sql
SELECT
    DATE_TRUNC('month', event_time) AS month_start,
    COUNT(*)                        AS event_count,
    SUM(value)                      AS monthly_revenue
FROM events
GROUP BY DATE_TRUNC('month', event_time)
ORDER BY month_start;
```

### Weekly Active Users (WAU)

```sql
SELECT
    user_id,
    COUNT(DISTINCT TO_DATE(event_time)) AS active_days,
    MIN(event_time)                     AS first_seen,
    MAX(event_time)                     AS last_seen
FROM events
WHERE event_time >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY user_id
ORDER BY active_days DESC;
```

### Year-over-Year comparison

```sql
SELECT
    MONTH(sale_date)                                          AS month_num,
    SUM(amount) FILTER (WHERE YEAR(sale_date) = 2024)        AS revenue_2024,
    SUM(amount) FILTER (WHERE YEAR(sale_date) = 2023)        AS revenue_2023,
    ROUND(
        (SUM(amount) FILTER (WHERE YEAR(sale_date) = 2024)
         - SUM(amount) FILTER (WHERE YEAR(sale_date) = 2023))
        / NULLIF(SUM(amount) FILTER (WHERE YEAR(sale_date) = 2023), 0) * 100,
        1
    ) AS yoy_pct
FROM sales
WHERE YEAR(sale_date) IN (2023, 2024)
GROUP BY MONTH(sale_date)
ORDER BY month_num;
```

### Rolling 7-day total (sliding window on aggregated daily data)

```sql
WITH daily AS (
    SELECT TO_DATE(event_time) AS day, SUM(value) AS daily_total
    FROM events
    GROUP BY TO_DATE(event_time)
)
SELECT
    day,
    daily_total,
    SUM(daily_total) OVER (
        ORDER BY day
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_total
FROM daily
ORDER BY day;
```

### Custom N-minute buckets

```sql
-- 15-minute buckets using integer arithmetic
SELECT
    TIMESTAMP(FLOOR(UNIX_TIMESTAMP(event_time) / (15 * 60)) * (15 * 60)) AS bucket_15m,
    COUNT(*) AS event_count
FROM events
GROUP BY FLOOR(UNIX_TIMESTAMP(event_time) / (15 * 60))
ORDER BY bucket_15m;
```

---

## :material-magnify: Behavior Notes

1. `DATE_TRUNC` accepts a string grain (`'hour'`, `'day'`, `'week'`, `'month'`, `'quarter'`, `'year'`) — it is the most readable and portable option.
2. `WEEKOFYEAR` follows ISO 8601 — week 1 is the week containing the first Thursday of the year.
3. `FILTER (WHERE ...)` inside aggregates is the cleanest way to build YoY or side-by-side period comparisons without pivoting.
4. For sub-minute buckets, use `FLOOR(UNIX_TIMESTAMP(ts) / bucket_seconds)` arithmetic.
5. Always `ORDER BY` the time column in the outer query — Spark does not guarantee order within `GROUP BY`.

