# :material-chart-line-variant: Peak Detection

Find **maximum traffic, peak CPU, highest sales hours** — identify local and global
peaks in time series data for capacity planning, alerting, and trend analysis.

---

## :material-sitemap: Detection Flow

```mermaid
flowchart LR
    RAW[Time Series Data] --> RANK[Rank Values\nROW_NUMBER · DENSE_RANK]
    RANK --> GLOBAL[Global Peaks\nMAX per partition]
    GLOBAL --> LOCAL[Local Peaks\nLAG/LEAD comparison]
    LOCAL --> CONTEXT[Peak Context\nDuration · Frequency · Severity]

    style RAW fill:#e3f2fd,stroke:#1e88e5
    style RANK fill:#e8f5e9,stroke:#43a047
    style GLOBAL fill:#fff3e0,stroke:#fb8c00
    style CONTEXT fill:#fce4ec,stroke:#e53935
```

---

## :material-code-tags: Syntax

### Sample data

Web traffic sampled every 10–15 minutes with natural variance within each hour.
CPU usage sampled every 5–10 minutes reflecting real monitoring agents.

```sql
CREATE OR REPLACE TEMP VIEW metrics AS
SELECT * FROM VALUES
  -- web_traffic: requests per minute, sampled every ~10-15 min
  ('web_traffic',  TIMESTAMP '2024-03-01 00:00', 1200),
  ('web_traffic',  TIMESTAMP '2024-03-01 00:10', 1180),
  ('web_traffic',  TIMESTAMP '2024-03-01 00:25', 1050),
  ('web_traffic',  TIMESTAMP '2024-03-01 00:40', 990),
  ('web_traffic',  TIMESTAMP '2024-03-01 01:00', 980),
  ('web_traffic',  TIMESTAMP '2024-03-01 01:15', 920),
  ('web_traffic',  TIMESTAMP '2024-03-01 01:30', 870),
  ('web_traffic',  TIMESTAMP '2024-03-01 01:50', 710),
  ('web_traffic',  TIMESTAMP '2024-03-01 02:00', 650),
  ('web_traffic',  TIMESTAMP '2024-03-01 02:20', 580),
  ('web_traffic',  TIMESTAMP '2024-03-01 02:45', 520),
  ('web_traffic',  TIMESTAMP '2024-03-01 03:00', 420),
  ('web_traffic',  TIMESTAMP '2024-03-01 03:30', 390),
  ('web_traffic',  TIMESTAMP '2024-03-01 04:00', 380),
  ('web_traffic',  TIMESTAMP '2024-03-01 04:20', 400),
  ('web_traffic',  TIMESTAMP '2024-03-01 04:45', 470),
  ('web_traffic',  TIMESTAMP '2024-03-01 05:00', 510),
  ('web_traffic',  TIMESTAMP '2024-03-01 05:15', 580),
  ('web_traffic',  TIMESTAMP '2024-03-01 05:30', 650),
  ('web_traffic',  TIMESTAMP '2024-03-01 05:50', 780),
  ('web_traffic',  TIMESTAMP '2024-03-01 06:00', 890),
  ('web_traffic',  TIMESTAMP '2024-03-01 06:10', 960),
  ('web_traffic',  TIMESTAMP '2024-03-01 06:25', 1050),
  ('web_traffic',  TIMESTAMP '2024-03-01 06:40', 1180),
  ('web_traffic',  TIMESTAMP '2024-03-01 07:00', 1450),
  ('web_traffic',  TIMESTAMP '2024-03-01 07:10', 1520),
  ('web_traffic',  TIMESTAMP '2024-03-01 07:20', 1680),
  ('web_traffic',  TIMESTAMP '2024-03-01 07:35', 1850),
  ('web_traffic',  TIMESTAMP '2024-03-01 07:50', 1980),
  ('web_traffic',  TIMESTAMP '2024-03-01 08:00', 2100),
  ('web_traffic',  TIMESTAMP '2024-03-01 08:15', 2350),
  ('web_traffic',  TIMESTAMP '2024-03-01 08:30', 2580),
  ('web_traffic',  TIMESTAMP '2024-03-01 08:45', 2720),
  ('web_traffic',  TIMESTAMP '2024-03-01 09:00', 2850),
  ('web_traffic',  TIMESTAMP '2024-03-01 09:10', 2920),
  ('web_traffic',  TIMESTAMP '2024-03-01 09:20', 3050),
  ('web_traffic',  TIMESTAMP '2024-03-01 09:40', 3120),
  ('web_traffic',  TIMESTAMP '2024-03-01 10:00', 3200),
  ('web_traffic',  TIMESTAMP '2024-03-01 10:15', 3280),
  ('web_traffic',  TIMESTAMP '2024-03-01 10:30', 3150),
  ('web_traffic',  TIMESTAMP '2024-03-01 10:45', 3050),
  ('web_traffic',  TIMESTAMP '2024-03-01 11:00', 3050),
  ('web_traffic',  TIMESTAMP '2024-03-01 11:20', 2980),
  ('web_traffic',  TIMESTAMP '2024-03-01 11:40', 2920),
  ('web_traffic',  TIMESTAMP '2024-03-01 12:00', 2900),
  ('web_traffic',  TIMESTAMP '2024-03-01 12:15', 2850),
  ('web_traffic',  TIMESTAMP '2024-03-01 12:30', 2950),
  ('web_traffic',  TIMESTAMP '2024-03-01 12:45', 3020),
  ('web_traffic',  TIMESTAMP '2024-03-01 13:00', 3100),
  ('web_traffic',  TIMESTAMP '2024-03-01 13:15', 3180),
  ('web_traffic',  TIMESTAMP '2024-03-01 13:30', 3250),
  ('web_traffic',  TIMESTAMP '2024-03-01 13:45', 3320),
  ('web_traffic',  TIMESTAMP '2024-03-01 14:00', 3400),
  ('web_traffic',  TIMESTAMP '2024-03-01 14:10', 3450),
  ('web_traffic',  TIMESTAMP '2024-03-01 14:20', 3380),
  ('web_traffic',  TIMESTAMP '2024-03-01 14:40', 3250),
  ('web_traffic',  TIMESTAMP '2024-03-01 15:00', 3150),
  ('web_traffic',  TIMESTAMP '2024-03-01 15:15', 3050),
  ('web_traffic',  TIMESTAMP '2024-03-01 15:30', 2950),
  ('web_traffic',  TIMESTAMP '2024-03-01 15:50', 2850),
  ('web_traffic',  TIMESTAMP '2024-03-01 16:00', 2800),
  ('web_traffic',  TIMESTAMP '2024-03-01 16:20', 2700),
  ('web_traffic',  TIMESTAMP '2024-03-01 16:45', 2650),
  ('web_traffic',  TIMESTAMP '2024-03-01 17:00', 2600),
  ('web_traffic',  TIMESTAMP '2024-03-01 17:15', 2500),
  ('web_traffic',  TIMESTAMP '2024-03-01 17:30', 2380),
  ('web_traffic',  TIMESTAMP '2024-03-01 17:50', 2250),
  ('web_traffic',  TIMESTAMP '2024-03-01 18:00', 2200),
  ('web_traffic',  TIMESTAMP '2024-03-01 18:20', 2100),
  ('web_traffic',  TIMESTAMP '2024-03-01 18:45', 1980),
  ('web_traffic',  TIMESTAMP '2024-03-01 19:00', 1900),
  ('web_traffic',  TIMESTAMP '2024-03-01 19:30', 1800),
  ('web_traffic',  TIMESTAMP '2024-03-01 20:00', 1700),
  ('web_traffic',  TIMESTAMP '2024-03-01 20:30', 1600),
  ('web_traffic',  TIMESTAMP '2024-03-01 21:00', 1500),
  ('web_traffic',  TIMESTAMP '2024-03-01 21:30', 1420),
  ('web_traffic',  TIMESTAMP '2024-03-01 22:00', 1350),
  ('web_traffic',  TIMESTAMP '2024-03-01 22:30', 1250),
  ('web_traffic',  TIMESTAMP '2024-03-01 23:00', 1100),
  ('web_traffic',  TIMESTAMP '2024-03-01 23:30', 1050),

  -- cpu_usage: % utilisation sampled every ~5-10 min
  ('cpu_usage',    TIMESTAMP '2024-03-01 08:00', 45.2),
  ('cpu_usage',    TIMESTAMP '2024-03-01 08:05', 48.1),
  ('cpu_usage',    TIMESTAMP '2024-03-01 08:10', 52.3),
  ('cpu_usage',    TIMESTAMP '2024-03-01 08:20', 55.8),
  ('cpu_usage',    TIMESTAMP '2024-03-01 08:30', 58.0),
  ('cpu_usage',    TIMESTAMP '2024-03-01 08:40', 60.5),
  ('cpu_usage',    TIMESTAMP '2024-03-01 08:50', 61.9),
  ('cpu_usage',    TIMESTAMP '2024-03-01 09:00', 62.8),
  ('cpu_usage',    TIMESTAMP '2024-03-01 09:10', 66.4),
  ('cpu_usage',    TIMESTAMP '2024-03-01 09:20', 70.1),
  ('cpu_usage',    TIMESTAMP '2024-03-01 09:30', 73.5),
  ('cpu_usage',    TIMESTAMP '2024-03-01 09:45', 76.2),
  ('cpu_usage',    TIMESTAMP '2024-03-01 10:00', 78.5),
  ('cpu_usage',    TIMESTAMP '2024-03-01 10:10', 80.2),
  ('cpu_usage',    TIMESTAMP '2024-03-01 10:20', 82.7),
  ('cpu_usage',    TIMESTAMP '2024-03-01 10:30', 84.0),
  ('cpu_usage',    TIMESTAMP '2024-03-01 10:45', 83.5),
  ('cpu_usage',    TIMESTAMP '2024-03-01 11:00', 85.3),
  ('cpu_usage',    TIMESTAMP '2024-03-01 11:10', 86.1),
  ('cpu_usage',    TIMESTAMP '2024-03-01 11:20', 84.8),
  ('cpu_usage',    TIMESTAMP '2024-03-01 11:30', 82.5),
  ('cpu_usage',    TIMESTAMP '2024-03-01 11:45', 78.0),
  ('cpu_usage',    TIMESTAMP '2024-03-01 12:00', 72.1),
  ('cpu_usage',    TIMESTAMP '2024-03-01 12:15', 70.5),
  ('cpu_usage',    TIMESTAMP '2024-03-01 12:30', 69.2),
  ('cpu_usage',    TIMESTAMP '2024-03-01 12:45', 68.0),
  ('cpu_usage',    TIMESTAMP '2024-03-01 13:00', 68.9),
  ('cpu_usage',    TIMESTAMP '2024-03-01 13:15', 72.3),
  ('cpu_usage',    TIMESTAMP '2024-03-01 13:30', 78.8),
  ('cpu_usage',    TIMESTAMP '2024-03-01 13:45', 85.2),
  ('cpu_usage',    TIMESTAMP '2024-03-01 14:00', 91.7),
  ('cpu_usage',    TIMESTAMP '2024-03-01 14:05', 93.4),
  ('cpu_usage',    TIMESTAMP '2024-03-01 14:10', 95.1),
  ('cpu_usage',    TIMESTAMP '2024-03-01 14:20', 92.8),
  ('cpu_usage',    TIMESTAMP '2024-03-01 14:30', 90.5),
  ('cpu_usage',    TIMESTAMP '2024-03-01 14:45', 89.0),
  ('cpu_usage',    TIMESTAMP '2024-03-01 15:00', 88.4),
  ('cpu_usage',    TIMESTAMP '2024-03-01 15:15', 85.9),
  ('cpu_usage',    TIMESTAMP '2024-03-01 15:30', 82.3),
  ('cpu_usage',    TIMESTAMP '2024-03-01 15:45', 79.1),
  ('cpu_usage',    TIMESTAMP '2024-03-01 16:00', 76.2),
  ('cpu_usage',    TIMESTAMP '2024-03-01 16:15', 71.8),
  ('cpu_usage',    TIMESTAMP '2024-03-01 16:30', 66.5),
  ('cpu_usage',    TIMESTAMP '2024-03-01 16:45', 60.3),
  ('cpu_usage',    TIMESTAMP '2024-03-01 17:00', 55.1)
AS t(metric_name, ts, value);
```

!!! note "Pre-aggregate for hourly analysis"

    With multiple samples per hour, several queries below first aggregate to
    hourly granularity using `DATE_TRUNC('HOUR', ts)`. This mirrors real
    pipelines where raw data is at second/minute resolution but analysis
    targets hourly or daily buckets.

---

### Global peak per metric

Find the single highest value for each metric across all samples.

```sql
SELECT
    metric_name,
    ts                                             AS peak_time,
    value                                          AS peak_value
FROM (
    SELECT
        metric_name,
        ts,
        value,
        ROW_NUMBER() OVER (
            PARTITION BY metric_name
            ORDER BY value DESC
        )                                          AS rn
    FROM metrics
)
WHERE rn = 1;
-- Result:
-- +------------+------------------+-----------+
-- |metric_name |peak_time         |peak_value |
-- +------------+------------------+-----------+
-- |web_traffic |2024-03-01 14:10  |3450       |
-- |cpu_usage   |2024-03-01 14:10  |95.1       |
-- +------------+------------------+-----------+
```

---

### Top-N peaks per metric

Return the highest N readings (useful for capacity planning).

```sql
SELECT
    metric_name,
    ts,
    value,
    DENSE_RANK() OVER (
        PARTITION BY metric_name
        ORDER BY value DESC
    )                                              AS peak_rank
FROM metrics
QUALIFY peak_rank <= 5
ORDER BY metric_name, peak_rank;
```

!!! note "QUALIFY alternative"
    If `QUALIFY` is unavailable, wrap in a subquery and filter with `WHERE peak_rank <= 5`.

---

### Local peak detection (higher than neighbours)

A local peak is a point where the value is greater than both the preceding and
following values — identifying distinct spikes rather than just the global maximum.

With sub-hourly data, first aggregate to hourly to avoid detecting noise as peaks:

```sql
WITH hourly AS (
    SELECT
        metric_name,
        DATE_TRUNC('HOUR', ts)                     AS ts,
        MAX(value)                                 AS value
    FROM metrics
    GROUP BY metric_name, DATE_TRUNC('HOUR', ts)
),
neighbours AS (
    SELECT
        metric_name,
        ts,
        value,
        LAG(value, 1) OVER w                       AS prev_value,
        LEAD(value, 1) OVER w                      AS next_value
    FROM hourly
    WINDOW w AS (PARTITION BY metric_name ORDER BY ts)
)
SELECT
    metric_name,
    ts                                             AS peak_time,
    value                                          AS peak_value,
    value - prev_value                             AS rise,
    value - next_value                             AS fall
FROM neighbours
WHERE value > COALESCE(prev_value, value - 1)
  AND value > COALESCE(next_value, value - 1)
ORDER BY metric_name, value DESC;
-- Result (web_traffic):
-- |peak_time        |peak_value|rise |fall |
-- |2024-03-01 10:00 |3280      |400  |280  |  ← morning peak (10:15 spike)
-- |2024-03-01 14:00 |3450      |330  |400  |  ← afternoon peak (14:10 spike)
```

To detect peaks at raw (sub-hourly) granularity instead, skip the `hourly` CTE and
query `metrics` directly — but expect many more local peaks from minute-level noise.

---

### Peak hours analysis (recurring daily peaks)

Identify which hours consistently have the highest values across multiple days.
With sub-hourly samples, aggregate first to get per-hour stats:

```sql
WITH hourly_stats AS (
    SELECT
        metric_name,
        HOUR(ts)                                   AS hr,
        AVG(value)                                 AS avg_value,
        MAX(value)                                 AS max_value,
        MIN(value)                                 AS min_value,
        COUNT(*)                                   AS sample_count
    FROM metrics
    GROUP BY metric_name, HOUR(ts)
)
SELECT
    metric_name,
    hr                                             AS peak_hour,
    ROUND(avg_value, 2)                            AS avg_value,
    max_value,
    sample_count,
    DENSE_RANK() OVER (
        PARTITION BY metric_name
        ORDER BY avg_value DESC
    )                                              AS hour_rank
FROM hourly_stats
ORDER BY metric_name, avg_value DESC;
```

!!! tip "Sample count matters"

    Hours with more samples (4–7 readings) give more reliable averages than hours
    with only 1–2 readings. Check `sample_count` before drawing conclusions.

---

### Peak duration (how long above threshold)

Measure how long a metric stays above a critical threshold. With sub-hourly data,
duration is measured in minutes rather than hours for more precision:

```sql
WITH above_threshold AS (
    SELECT
        metric_name,
        ts,
        value,
        CASE WHEN value > 3000 THEN 1 ELSE 0 END  AS is_peak,
        SUM(CASE
            WHEN value > 3000 THEN 0 ELSE 1
        END) OVER (
            PARTITION BY metric_name
            ORDER BY ts
            ROWS UNBOUNDED PRECEDING
        )                                          AS peak_group
    FROM metrics
    WHERE metric_name = 'web_traffic'
)
SELECT
    MIN(ts)                                        AS peak_start,
    MAX(ts)                                        AS peak_end,
    COUNT(*)                                       AS samples_above,
    ROUND(
        (UNIX_TIMESTAMP(MAX(ts)) - UNIX_TIMESTAMP(MIN(ts))) / 60.0, 0
    )                                              AS duration_minutes,
    ROUND(AVG(value), 0)                           AS avg_during_peak,
    MAX(value)                                     AS max_during_peak
FROM above_threshold
WHERE is_peak = 1
GROUP BY peak_group
ORDER BY peak_start;
-- Result:
-- |peak_start       |peak_end         |samples|dur_min|avg_peak|max_peak|
-- |2024-03-01 09:20 |2024-03-01 11:00 |8      |100    |3110    |3280    |  ← morning
-- |2024-03-01 12:45 |2024-03-01 15:00 |11     |135    |3233    |3450    |  ← afternoon
```

!!! note "Sub-hourly precision"

    With 1 sample per hour, you can only say "above threshold for 5 hours."
    With 4+ samples per hour, you can pinpoint that the afternoon peak lasted
    exactly 135 minutes — from 12:45 to 15:00.

---

### Peaks relative to rolling baseline

Detect peaks as deviations from a rolling average (adaptive threshold).
With sub-hourly data, use a wider preceding window to capture ~1 hour of history:

```sql
WITH baseline AS (
    SELECT
        metric_name,
        ts,
        value,
        AVG(value) OVER (
            PARTITION BY metric_name
            ORDER BY ts
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        )                                          AS rolling_avg_excl,
        STDDEV(value) OVER (
            PARTITION BY metric_name
            ORDER BY ts
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        )                                          AS rolling_stddev_excl
    FROM metrics
)
SELECT
    metric_name,
    ts,
    value,
    ROUND(rolling_avg_excl, 2)                     AS baseline,
    ROUND(
        (value - rolling_avg_excl)
        / NULLIF(rolling_stddev_excl, 0), 2
    )                                              AS sigma_above,
    CASE
        WHEN value > rolling_avg_excl + 2 * rolling_stddev_excl
            THEN 'PEAK'
        ELSE 'NORMAL'
    END                                            AS status
FROM baseline
WHERE rolling_avg_excl IS NOT NULL
ORDER BY metric_name, ts;
```

!!! tip "Window size tuning"

    With ~4 samples/hour, `ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING` covers
    roughly the last 3 hours. Adjust based on your sampling frequency:
    
    - 1 sample/min → use `ROWS BETWEEN 180 PRECEDING` for 3 hours
    - 1 sample/5 min → use `ROWS BETWEEN 36 PRECEDING` for 3 hours
    - 1 sample/15 min → use `ROWS BETWEEN 12 PRECEDING` for 3 hours

---

## :material-information-outline: Key Concepts

| Technique | Purpose | Best For |
|-----------|---------|----------|
| `ROW_NUMBER ORDER BY value DESC` | Global top-N | "What was the highest ever?" |
| `LAG/LEAD` comparison | Local peaks (higher than neighbours) | Finding distinct spikes |
| `GROUP BY HOUR(ts)` | Recurring peak hours | Capacity scheduling |
| Gaps-and-islands grouping | Peak duration / episodes | "How long was it elevated?" |
| Rolling baseline + σ | Adaptive peak detection | Variable baselines (weekday vs weekend) |

!!! tip "Exclude current row from baseline"
    Use `ROWS BETWEEN n PRECEDING AND 1 PRECEDING` to exclude the current row
    from the baseline calculation — prevents the peak itself from inflating the
    threshold.

!!! warning "Multiple peaks at same value"
    `ROW_NUMBER` arbitrarily picks one; use `DENSE_RANK` to return all ties
    at the peak value.

---

## :material-lightbulb-outline: When to Use

| Scenario | Approach |
|----------|----------|
| Maximum traffic hour | Global peak with `ROW_NUMBER` |
| CPU capacity planning | Top-N peaks + peak duration |
| Sales flash detection | Local peaks (LAG/LEAD) |
| SLA breach duration | Gaps-and-islands above threshold |
| Auto-scaling triggers | Rolling baseline + adaptive σ threshold |
| Recurring bottlenecks | Peak hours aggregation across days |

---

## :material-speedometer: Performance Notes

| Tip | Reason |
|-----|--------|
| Filter date range before peak detection | Reduces window sort cost |
| Partition by metric/entity | Enables parallelism, avoids global sort |
| Use `QUALIFY` for Top-N | Avoids extra subquery wrapping |
| Pre-aggregate to target granularity | Hourly peaks don't need minute-level data |
| Materialise rolling baselines | Reuse across multiple detection rules |

---

## :material-arrow-right: Related

- [Rolling Statistics](rolling_statistics.md) — stddev, variance, Z-scores for anomaly detection
- [Forecast Features](forecast_features.md) — lag features for ML-based peak prediction
- [Seasonality Detection](seasonality_detection.md) — recurring peak patterns by period
- [Gap Filling](gap_filling.md) — ensure no missing timestamps before peak analysis
