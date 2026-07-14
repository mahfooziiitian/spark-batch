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
-- Multi-day event stream (spans 10 days, 3 users, 3 event types)
CREATE OR REPLACE TEMP VIEW events AS
SELECT * FROM VALUES
  -- Day 1: Mon 2025-07-14
  (1001, 1, TIMESTAMP('2025-07-14 08:12:00'), 'click',     50),
  (1002, 2, TIMESTAMP('2025-07-14 09:30:00'), 'view',      10),
  (1003, 1, TIMESTAMP('2025-07-14 08:45:00'), 'click',     35),
  -- Day 2: Tue 2025-07-15
  (1004, 3, TIMESTAMP('2025-07-15 10:05:00'), 'purchase', 200),
  (1005, 2, TIMESTAMP('2025-07-15 10:22:00'), 'click',     40),
  (1006, 1, TIMESTAMP('2025-07-15 14:00:00'), 'view',      15),
  -- Day 3: Wed 2025-07-16
  (1007, 2, TIMESTAMP('2025-07-16 11:10:00'), 'click',     60),
  (1008, 3, TIMESTAMP('2025-07-16 11:45:00'), 'purchase', 150),
  (1009, 1, TIMESTAMP('2025-07-16 16:30:00'), 'click',     25),
  -- Day 5: Fri 2025-07-18 (gap on Thu)
  (1010, 1, TIMESTAMP('2025-07-18 09:00:00'), 'purchase', 300),
  (1011, 2, TIMESTAMP('2025-07-18 09:15:00'), 'click',     45),
  (1012, 3, TIMESTAMP('2025-07-18 13:50:00'), 'view',      20),
  -- Day 8: Mon 2025-07-21 (week 2)
  (1013, 1, TIMESTAMP('2025-07-21 08:00:00'), 'click',     55),
  (1014, 2, TIMESTAMP('2025-07-21 10:30:00'), 'purchase', 180),
  (1015, 3, TIMESTAMP('2025-07-21 15:20:00'), 'view',      10),
  -- Day 10: Wed 2025-07-23
  (1016, 1, TIMESTAMP('2025-07-23 07:50:00'), 'click',     70),
  (1017, 2, TIMESTAMP('2025-07-23 12:00:00'), 'purchase', 250),
  (1018, 3, TIMESTAMP('2025-07-23 12:10:00'), 'click',     30)
AS events(event_id, user_id, event_time, event_type, value);
```

```sql
-- Sales data for Year-over-Year examples (2 years, monthly)
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  (DATE('2023-01-15'), 'electronics', 12000),
  (DATE('2023-02-10'), 'electronics', 14500),
  (DATE('2023-03-22'), 'clothing',     8200),
  (DATE('2023-04-05'), 'electronics', 11000),
  (DATE('2023-05-18'), 'clothing',     9500),
  (DATE('2023-06-30'), 'electronics', 15800),
  (DATE('2024-01-12'), 'electronics', 13500),
  (DATE('2024-02-08'), 'clothing',    16200),
  (DATE('2024-03-19'), 'electronics',  9800),
  (DATE('2024-04-25'), 'clothing',    12400),
  (DATE('2024-05-14'), 'electronics', 10200),
  (DATE('2024-06-28'), 'clothing',    17500)
AS sales(sale_date, category, amount);
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

??? success "Expected output"

    | hour_bucket         | event_count | total_value |
    |---------------------|-------------|-------------|
    | 2025-07-14 08:00:00 | 2           | 85          |
    | 2025-07-14 09:00:00 | 1           | 10          |
    | 2025-07-15 10:00:00 | 2           | 240         |
    | 2025-07-15 14:00:00 | 1           | 15          |
    | 2025-07-16 11:00:00 | 2           | 210         |
    | ...                 | ...         | ...         |

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

??? success "Expected output"

    | user_id | event_date | daily_count | daily_value |
    |---------|------------|-------------|-------------|
    | 1       | 2025-07-14 | 2           | 85          |
    | 1       | 2025-07-15 | 1           | 15          |
    | 1       | 2025-07-16 | 1           | 25          |
    | 1       | 2025-07-18 | 1           | 300         |
    | 1       | 2025-07-21 | 1           | 55          |
    | 1       | 2025-07-23 | 1           | 70          |
    | 2       | 2025-07-14 | 1           | 10          |
    | 2       | 2025-07-15 | 1           | 40          |
    | 2       | 2025-07-16 | 1           | 60          |
    | ...     | ...        | ...         | ...         |

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

??? success "Expected output"

    | week_start          | iso_week | event_count | weekly_value |
    |---------------------|----------|-------------|--------------|
    | 2025-07-14 00:00:00 | 29       | 12          | 950          |
    | 2025-07-21 00:00:00 | 30       | 6           | 595          |

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

??? success "Expected output"

    | month_start         | event_count | monthly_revenue |
    |---------------------|-------------|-----------------|
    | 2025-07-01 00:00:00 | 18          | 1545            |

### Weekly Active Users (WAU)

```sql
-- Active days per user in the last 7 days from reference date
SELECT
    user_id,
    COUNT(DISTINCT TO_DATE(event_time)) AS active_days,
    MIN(event_time)                     AS first_seen,
    MAX(event_time)                     AS last_seen
FROM events
WHERE event_time >= DATE('2025-07-18') - INTERVAL 7 DAYS
GROUP BY user_id
ORDER BY active_days DESC;
```

??? success "Expected output (reference: 2025-07-18)"

    | user_id | active_days | first_seen          | last_seen           |
    |---------|-------------|---------------------|---------------------|
    | 1       | 4           | 2025-07-14 08:12:00 | 2025-07-18 09:00:00 |
    | 2       | 4           | 2025-07-14 09:30:00 | 2025-07-18 09:15:00 |
    | 3       | 3           | 2025-07-15 10:05:00 | 2025-07-18 13:50:00 |

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

??? success "Expected output"

    | month_num | revenue_2024 | revenue_2023 | yoy_pct |
    |-----------|--------------|--------------|---------|
    | 1         | 13500        | 12000        | 12.5    |
    | 2         | 16200        | 14500        | 11.7    |
    | 3         | 9800         | 8200         | 19.5    |
    | 4         | 12400        | 11000        | 12.7    |
    | 5         | 10200        | 9500         | 7.4     |
    | 6         | 17500        | 15800        | 10.8    |

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

??? success "Expected output"

    | day        | daily_total | rolling_7d_total |
    |------------|-------------|------------------|
    | 2025-07-14 | 95          | 95               |
    | 2025-07-15 | 255         | 350              |
    | 2025-07-16 | 235         | 585              |
    | 2025-07-18 | 365         | 950              |
    | 2025-07-21 | 245         | 610              |
    | 2025-07-23 | 350         | 960              |

### Custom N-minute buckets

```sql
-- 15-minute buckets using integer arithmetic
SELECT
    TIMESTAMP(FLOOR(UNIX_TIMESTAMP(event_time) / (15 * 60)) * (15 * 60)) AS bucket_15m,
    COUNT(*) AS event_count,
    SUM(value) AS bucket_value
FROM events
GROUP BY FLOOR(UNIX_TIMESTAMP(event_time) / (15 * 60))
ORDER BY bucket_15m;
```

??? success "Expected output (first 5 rows)"

    | bucket_15m          | event_count | bucket_value |
    |---------------------|-------------|--------------|
    | 2025-07-14 08:00:00 | 1           | 50           |
    | 2025-07-14 08:45:00 | 1           | 35           |
    | 2025-07-14 09:30:00 | 1           | 10           |
    | 2025-07-15 10:00:00 | 1           | 200          |
    | 2025-07-15 10:15:00 | 1           | 40           |

---

## :material-magnify: Behavior Notes

1. `DATE_TRUNC` accepts a string grain (`'hour'`, `'day'`, `'week'`, `'month'`, `'quarter'`, `'year'`) — it is the most readable and portable option.
2. `WEEKOFYEAR` follows ISO 8601 — week 1 is the week containing the first Thursday of the year.
3. `FILTER (WHERE ...)` inside aggregates is the cleanest way to build YoY or side-by-side period comparisons without pivoting.
4. For sub-minute buckets, use `FLOOR(UNIX_TIMESTAMP(ts) / bucket_seconds)` arithmetic.
5. Always `ORDER BY` the time column in the outer query — Spark does not guarantee order within `GROUP BY`.

