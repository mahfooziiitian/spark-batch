# :material-server: Capacity Planning

Analyze **peak usage, average usage, and growth rate** — forecast when resources will
hit capacity limits and plan infrastructure scaling decisions.

______________________________________________________________________

## :material-animation-play: Interactive Demo

Hover each weekly point to compare observed utilization with the projected trend and capacity threshold. The dashed line shows when current growth would exhaust headroom.

<div id="viz-capacity" class="ts-viz"></div>

*Purple = observed usage. Amber dashed = projected trend. Red marks the capacity limit and projected breach point.*

______________________________________________________________________

## :material-sitemap: Planning Flow

```mermaid
flowchart LR
    HIST[Historical Usage Data] --> STATS[Usage Statistics\nPeak · Avg · P95]
    STATS --> GROWTH[Growth Rate\nMoM · Trend slope]
    GROWTH --> FORECAST[Capacity Forecast\nProjected exhaustion date]
    FORECAST --> ACTION[Planning Actions\nScale · Optimise · Budget]

    style HIST fill:#e3f2fd,stroke:#1e88e5
    style STATS fill:#e8f5e9,stroke:#43a047
    style GROWTH fill:#fff3e0,stroke:#fb8c00
    style ACTION fill:#fce4ec,stroke:#e53935
```

______________________________________________________________________

## :material-code-tags: Syntax

### Sample data

```sql
CREATE OR REPLACE TEMP VIEW resource_usage AS
SELECT * FROM VALUES
  ('storage',  DATE '2024-01-01', 520,  1000),
  ('storage',  DATE '2024-02-01', 545,  1000),
  ('storage',  DATE '2024-03-01', 580,  1000),
  ('storage',  DATE '2024-04-01', 610,  1000),
  ('storage',  DATE '2024-05-01', 650,  1000),
  ('storage',  DATE '2024-06-01', 690,  1000),
  ('storage',  DATE '2024-07-01', 720,  1000),
  ('storage',  DATE '2024-08-01', 760,  1000),
  ('storage',  DATE '2024-09-01', 800,  1000),
  ('storage',  DATE '2024-10-01', 830,  1000),
  ('storage',  DATE '2024-11-01', 870,  1000),
  ('storage',  DATE '2024-12-01', 910,  1000),
  ('compute',  DATE '2024-01-01', 45,   100),
  ('compute',  DATE '2024-02-01', 48,   100),
  ('compute',  DATE '2024-03-01', 52,   100),
  ('compute',  DATE '2024-04-01', 55,   100),
  ('compute',  DATE '2024-05-01', 60,   100),
  ('compute',  DATE '2024-06-01', 63,   100),
  ('compute',  DATE '2024-07-01', 68,   100),
  ('compute',  DATE '2024-08-01', 72,   100),
  ('compute',  DATE '2024-09-01', 75,   100),
  ('compute',  DATE '2024-10-01', 78,   100),
  ('compute',  DATE '2024-11-01', 82,   100),
  ('compute',  DATE '2024-12-01', 85,   100)
AS t(resource_type, measure_date, usage_value, capacity_limit);
```

______________________________________________________________________

### Peak, average, and percentile usage

Summarise usage distribution per resource.

```sql
SELECT
    resource_type,
    capacity_limit,
    ROUND(AVG(usage_value), 1)                     AS avg_usage,
    MAX(usage_value)                               AS peak_usage,
    MIN(usage_value)                               AS min_usage,
    ROUND(PERCENTILE_APPROX(usage_value, 0.95), 1)
                                                   AS p95_usage,
    ROUND(AVG(usage_value) * 100.0 / capacity_limit, 1)
                                                   AS avg_util_pct,
    ROUND(MAX(usage_value) * 100.0 / capacity_limit, 1)
                                                   AS peak_util_pct
FROM resource_usage
GROUP BY resource_type, capacity_limit
ORDER BY peak_util_pct DESC;
-- Result:
-- |resource_type|capacity|avg_usage|peak|p95  |avg_util%|peak_util%|
-- |storage      |1000    |707.1    |910 |898.5|70.7     |91.0      |
-- |compute      |100     |65.3     |85  |83.5 |65.3     |85.0      |
```

______________________________________________________________________

### Month-over-month growth rate

Calculate the rate of increase to project future consumption.

```sql
SELECT
    resource_type,
    measure_date,
    usage_value,
    capacity_limit,
    LAG(usage_value, 1) OVER w                     AS prev_month_usage,
    usage_value - LAG(usage_value, 1) OVER w       AS absolute_growth,
    ROUND(
        (usage_value - LAG(usage_value, 1) OVER w) * 100.0
        / NULLIF(LAG(usage_value, 1) OVER w, 0),
        2
    )                                              AS mom_growth_pct,
    ROUND(usage_value * 100.0 / capacity_limit, 1)
                                                   AS current_util_pct
FROM resource_usage
WINDOW w AS (PARTITION BY resource_type ORDER BY measure_date)
ORDER BY resource_type, measure_date;
-- Result (storage):
-- |measure_date|usage|prev |growth|mom_pct|util_pct|
-- |2024-01-01  |520  |NULL |NULL  |NULL   |52.0    |
-- |2024-02-01  |545  |520  |25    |4.81   |54.5    |
-- |2024-12-01  |910  |870  |40    |4.60   |91.0    |
```

______________________________________________________________________

### Linear growth projection (capacity exhaustion date)

Estimate when the resource will reach its limit using average growth rate.

```sql
WITH growth_stats AS (
    SELECT
        resource_type,
        capacity_limit,
        MAX(usage_value)                           AS latest_usage,
        MAX(measure_date)                          AS latest_date,
        -- Average monthly absolute growth
        ROUND(
            (MAX(usage_value) - MIN(usage_value))
            * 1.0 / (MONTHS_BETWEEN(MAX(measure_date), MIN(measure_date))),
            2
        )                                          AS avg_monthly_growth
    FROM resource_usage
    GROUP BY resource_type, capacity_limit
)
SELECT
    resource_type,
    capacity_limit,
    latest_usage,
    latest_date,
    avg_monthly_growth,
    capacity_limit - latest_usage                  AS remaining_capacity,
    -- Months until full
    ROUND(
        (capacity_limit - latest_usage) / NULLIF(avg_monthly_growth, 0),
        1
    )                                              AS months_to_full,
    -- Projected exhaustion date
    ADD_MONTHS(
        latest_date,
        CAST(CEIL(
            (capacity_limit - latest_usage) / NULLIF(avg_monthly_growth, 0)
        ) AS INT)
    )                                              AS projected_full_date
FROM growth_stats
ORDER BY months_to_full ASC;
-- Result:
-- |resource_type|capacity|latest|growth/mo|remaining|months_to_full|full_date |
-- |storage      |1000    |910   |35.45    |90       |2.5           |2025-03-01|
-- |compute      |100     |85    |3.64     |15       |4.1           |2025-05-01|
```

______________________________________________________________________

### Headroom analysis with threshold alerts

Flag resources approaching capacity at configurable warning levels.

```sql
WITH current_state AS (
    SELECT
        resource_type,
        capacity_limit,
        usage_value                                AS current_usage,
        measure_date,
        ROW_NUMBER() OVER (
            PARTITION BY resource_type
            ORDER BY measure_date DESC
        )                                          AS rn
    FROM resource_usage
)
SELECT
    resource_type,
    current_usage,
    capacity_limit,
    capacity_limit - current_usage                 AS headroom,
    ROUND(current_usage * 100.0 / capacity_limit, 1)
                                                   AS util_pct,
    CASE
        WHEN current_usage * 100.0 / capacity_limit >= 90
            THEN 'CRITICAL'
        WHEN current_usage * 100.0 / capacity_limit >= 75
            THEN 'WARNING'
        WHEN current_usage * 100.0 / capacity_limit >= 60
            THEN 'WATCH'
        ELSE 'HEALTHY'
    END                                            AS status
FROM current_state
WHERE rn = 1
ORDER BY util_pct DESC;
-- Result:
-- |resource_type|current|capacity|headroom|util_pct|status  |
-- |storage      |910    |1000    |90      |91.0    |CRITICAL|
-- |compute      |85     |100     |15      |85.0    |WARNING |
```

______________________________________________________________________

### Rolling growth rate with acceleration

Detect whether growth is accelerating or decelerating.

```sql
WITH monthly_growth AS (
    SELECT
        resource_type,
        measure_date,
        usage_value,
        usage_value - LAG(usage_value, 1) OVER w   AS growth,
        LAG(usage_value - LAG(usage_value, 1) OVER w, 1) OVER w
                                                   AS prev_growth
    FROM resource_usage
    WINDOW w AS (PARTITION BY resource_type ORDER BY measure_date)
)
SELECT
    resource_type,
    measure_date,
    usage_value,
    growth,
    ROUND(AVG(growth) OVER (
        PARTITION BY resource_type
        ORDER BY measure_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                          AS rolling_avg_growth_3m,
    CASE
        WHEN growth > prev_growth THEN 'ACCELERATING'
        WHEN growth < prev_growth THEN 'DECELERATING'
        ELSE 'STEADY'
    END                                            AS growth_trend
FROM monthly_growth
WHERE growth IS NOT NULL
ORDER BY resource_type, measure_date;
```

______________________________________________________________________

### Capacity planning summary report

Combine all metrics into a single executive view.

```sql
WITH stats AS (
    SELECT
        resource_type,
        capacity_limit,
        MAX(usage_value)                           AS latest_usage,
        MAX(measure_date)                          AS latest_date,
        AVG(usage_value)                           AS avg_usage,
        ROUND(
            (MAX(usage_value) - MIN(usage_value))
            / MONTHS_BETWEEN(MAX(measure_date), MIN(measure_date)),
            2
        )                                          AS avg_growth_per_month,
        ROUND(PERCENTILE_APPROX(usage_value, 0.95), 1)
                                                   AS p95_usage
    FROM resource_usage
    GROUP BY resource_type, capacity_limit
)
SELECT
    resource_type,
    capacity_limit,
    latest_usage,
    ROUND(avg_usage, 1)                            AS avg_usage,
    p95_usage,
    avg_growth_per_month,
    ROUND(latest_usage * 100.0 / capacity_limit, 1)
                                                   AS current_util_pct,
    capacity_limit - latest_usage                  AS headroom,
    ROUND(
        (capacity_limit - latest_usage)
        / NULLIF(avg_growth_per_month, 0), 1
    )                                              AS months_remaining,
    CASE
        WHEN (capacity_limit - latest_usage)
             / NULLIF(avg_growth_per_month, 0) <= 3
            THEN 'URGENT'
        WHEN (capacity_limit - latest_usage)
             / NULLIF(avg_growth_per_month, 0) <= 6
            THEN 'PLAN NOW'
        ELSE 'MONITOR'
    END                                            AS action_required
FROM stats
ORDER BY months_remaining ASC;
```

______________________________________________________________________

## :material-information-outline: Key Concepts

| Metric           | Formula                          | Purpose                           |
| ---------------- | -------------------------------- | --------------------------------- |
| **Peak Usage**   | `MAX(usage)`                     | Worst-case capacity need          |
| **Avg Usage**    | `AVG(usage)`                     | Steady-state baseline             |
| **P95 Usage**    | `PERCENTILE_APPROX(usage, 0.95)` | Practical peak excluding outliers |
| **Growth Rate**  | `(latest - earliest) / months`   | Linear projection input           |
| **Headroom**     | `capacity - current`             | Buffer before exhaustion          |
| **Time to Full** | `headroom / growth_rate`         | Months until limit reached        |

!!! tip "Use P95 over peak for planning"

    Raw peak values may include one-time spikes. P95 gives a more realistic
    "practical maximum" that excludes extreme outliers, leading to better
    capacity decisions.

!!! warning "Linear vs exponential growth"

    Linear projection works for steady growth. If growth is accelerating
    (compound), use exponential projection: `months = log(capacity/current) / log(1 + growth_rate)`.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                   | Key Metrics                           |
| -------------------------- | ------------------------------------- |
| Storage expansion planning | Months to full, growth rate per month |
| Compute scaling decisions  | P95 utilization, peak headroom        |
| Database sizing            | Row count growth, query volume trends |
| Network bandwidth          | Peak throughput vs link capacity      |
| Cloud cost forecasting     | Usage growth × unit cost projection   |
| SLA risk assessment        | Headroom alerts at 75%/90% thresholds |

______________________________________________________________________

## :material-speedometer: Performance Notes

| Tip                                          | Reason                                            |
| -------------------------------------------- | ------------------------------------------------- |
| Pre-aggregate to daily/weekly granularity    | Reduces rows before window functions              |
| Partition by `resource_type`                 | Enables parallel computation per resource         |
| Use `MONTHS_BETWEEN` for growth rate         | Handles irregular month lengths correctly         |
| Materialise growth stats as a daily job      | Avoid recomputing over full history               |
| Filter to recent 12-24 months for projection | Older data may not reflect current growth pattern |

______________________________________________________________________

## :material-database-search: [Databricks] Real-World Example — System Tables

!!! note "[Databricks] Unity Catalog system tables"

    `system.compute.node_timeline` is a built-in Unity Catalog system table
    (no sample data setup needed) that captures real cluster node CPU and
    memory telemetry over time. An account admin must grant `USE CATALOG`
    on `system`, `USE SCHEMA` on `system.compute`, and `SELECT` on
    `system.compute.node_timeline` before these queries will return rows.

### Capacity headroom forecast from node telemetry

```sql
-- [Databricks] Requires SELECT on system.compute.node_timeline
WITH daily_usage AS (
    SELECT
        cluster_id,
        DATE(start_time) AS usage_date,
        ROUND(AVG(cpu_user_percent + cpu_system_percent), 1) AS avg_cpu_pct,
        ROUND(MAX(mem_used_percent), 1) AS peak_mem_pct
    FROM system.compute.node_timeline
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
    GROUP BY cluster_id, DATE(start_time)
),
growth_stats AS (
    SELECT
        cluster_id,
        MAX(usage_date) AS latest_date,
        ROUND(
            (MAX(avg_cpu_pct) - MIN(avg_cpu_pct))
            / NULLIF(DATEDIFF(MAX(usage_date), MIN(usage_date)), 0),
            2
        ) AS avg_daily_cpu_growth
    FROM daily_usage
    GROUP BY cluster_id
),
current_state AS (
    SELECT
        cluster_id,
        usage_date,
        avg_cpu_pct,
        peak_mem_pct,
        ROW_NUMBER() OVER (
            PARTITION BY cluster_id
            ORDER BY usage_date DESC
        ) AS rn
    FROM daily_usage
)
SELECT
    c.cluster_id,
    c.usage_date AS latest_date,
    c.avg_cpu_pct,
    c.peak_mem_pct,
    g.avg_daily_cpu_growth,
    ROUND(85 - c.avg_cpu_pct, 1) AS cpu_headroom_pct_points,
    CASE
        WHEN g.avg_daily_cpu_growth > 0 AND c.avg_cpu_pct < 85
            THEN DATE_ADD(
                c.usage_date,
                CAST(CEIL((85 - c.avg_cpu_pct) / g.avg_daily_cpu_growth) AS INT)
            )
    END AS projected_85_pct_date
FROM current_state AS c
JOIN growth_stats AS g
    ON c.cluster_id = g.cluster_id
WHERE c.rn = 1
ORDER BY projected_85_pct_date, c.cluster_id;
-- Result (illustrative):
-- cluster_id | latest_date | avg_cpu_pct | peak_mem_pct | avg_daily_cpu_growth | cpu_headroom_pct_points | projected_85_pct_date
-- ---------- | ----------- | ----------- | ------------ | -------------------- | ----------------------- | ---------------------
-- 0315-1015-abcd123 | 2024-07-02 | 73.4 | 88.1 | 0.42 | 11.6 | 2024-07-30
-- 0315-2045-efgh456 | 2024-07-02 | 81.7 | 91.5 | 0.18 | 3.3  | 2024-07-21
```

!!! tip "Same planning pattern, real operational telemetry"

    The same peak/average/growth workflow from the synthetic examples works
    directly on `system.compute.node_timeline`: aggregate to daily buckets,
    estimate growth, then project when real clusters will run out of safe
    headroom.

______________________________________________________________________

## :material-arrow-right: Related

- [Peak Detection](peak-detection.md) — identify maximum usage moments
- [Trend Detection](trend-detection.md) — spot growth acceleration or slowdown
- [Utilization Analysis](utilization-analysis.md) — busy/idle time measurement
- [Rolling Statistics](rolling-statistics.md) — variance and anomaly detection in usage
