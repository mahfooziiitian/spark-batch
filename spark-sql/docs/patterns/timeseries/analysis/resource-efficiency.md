# :material-speedometer: Resource Efficiency

Measure **CPU utilization, DBU efficiency, warehouse utilization, query efficiency,
and cache hit ratio** — quantify how well resources convert cost into useful work.

______________________________________________________________________

## :material-animation-play: Interactive Demo

Hover each point to compare utilization and spend, then contrast teal resources with red outliers that sit below the efficiency frontier.

<div id="viz-efficiency" class="ts-viz"></div>

*Utilization rises left-to-right while cost rises bottom-to-top. The dashed frontier marks a better cost-to-utilization trade-off than the red inefficient points.*

______________________________________________________________________

## :material-sitemap: Efficiency Framework

```mermaid
flowchart LR
    USAGE[Resource Usage Logs] --> RATIO[Efficiency Ratios\nOutput / Input]
    RATIO --> BENCH[Benchmarking\nCompare across resources]
    BENCH --> OPT[Optimisation\nRightsize · Tune · Cache]

    style USAGE fill:#e3f2fd,stroke:#1e88e5
    style RATIO fill:#e8f5e9,stroke:#43a047
    style OPT fill:#fce4ec,stroke:#e53935
```

______________________________________________________________________

## :material-code-tags: Syntax

### Sample data

```sql
CREATE OR REPLACE TEMP VIEW warehouse_metrics AS
SELECT * FROM VALUES
  ('wh_prod',  DATE '2024-03-01', 100, 75.5,  8.5,  12.0, 850,  720),
  ('wh_prod',  DATE '2024-03-02', 100, 80.2,  9.0,  12.0, 920,  810),
  ('wh_prod',  DATE '2024-03-03', 100, 62.0,  7.2,  12.0, 600,  480),
  ('wh_dev',   DATE '2024-03-01', 50,  30.1,  3.5,  10.0, 200,  140),
  ('wh_dev',   DATE '2024-03-02', 50,  25.8,  2.8,  10.0, 150,  90),
  ('wh_dev',   DATE '2024-03-03', 50,  45.0,  5.0,  10.0, 320,  250),
  ('wh_bi',    DATE '2024-03-01', 20,  18.5,  1.2,  8.0,  500,  475),
  ('wh_bi',    DATE '2024-03-02', 20,  19.0,  1.3,  8.0,  520,  500),
  ('wh_bi',    DATE '2024-03-03', 20,  17.8,  1.1,  8.0,  480,  460)
AS t(warehouse_id, metric_date, max_capacity_pct, avg_cpu_pct, dbu_consumed,
     uptime_hours, total_queries, cache_hits);
```

______________________________________________________________________

### CPU and DBU utilization

```sql
SELECT
    warehouse_id,
    metric_date,
    avg_cpu_pct,
    max_capacity_pct,
    ROUND(avg_cpu_pct / max_capacity_pct * 100, 1) AS cpu_efficiency_pct,
    dbu_consumed,
    uptime_hours,
    ROUND(dbu_consumed / uptime_hours, 3)          AS dbu_per_hour
FROM warehouse_metrics
ORDER BY warehouse_id, metric_date;
```

______________________________________________________________________

### Query efficiency (queries per DBU)

```sql
SELECT
    warehouse_id,
    metric_date,
    total_queries,
    dbu_consumed,
    ROUND(total_queries * 1.0 / NULLIF(dbu_consumed, 0), 1)
                                                   AS queries_per_dbu,
    ROUND(dbu_consumed / NULLIF(total_queries, 0) * 1000, 2)
                                                   AS cost_per_1000_queries
FROM warehouse_metrics
ORDER BY queries_per_dbu DESC;
```

______________________________________________________________________

### Cache hit ratio

```sql
SELECT
    warehouse_id,
    metric_date,
    total_queries,
    cache_hits,
    total_queries - cache_hits                     AS cache_misses,
    ROUND(cache_hits * 100.0 / NULLIF(total_queries, 0), 1)
                                                   AS cache_hit_pct,
    CASE
        WHEN cache_hits * 100.0 / NULLIF(total_queries, 0) >= 90 THEN 'EXCELLENT'
        WHEN cache_hits * 100.0 / NULLIF(total_queries, 0) >= 70 THEN 'GOOD'
        WHEN cache_hits * 100.0 / NULLIF(total_queries, 0) >= 50 THEN 'FAIR'
        ELSE 'POOR'
    END                                            AS cache_health
FROM warehouse_metrics
ORDER BY cache_hit_pct DESC;
```

______________________________________________________________________

### Warehouse efficiency scorecard

```sql
SELECT
    warehouse_id,
    ROUND(AVG(avg_cpu_pct), 1)                     AS avg_cpu,
    ROUND(AVG(dbu_consumed / uptime_hours), 3)     AS avg_dbu_per_hour,
    ROUND(AVG(total_queries * 1.0 / NULLIF(dbu_consumed, 0)), 1)
                                                   AS avg_queries_per_dbu,
    ROUND(AVG(cache_hits * 100.0 / NULLIF(total_queries, 0)), 1)
                                                   AS avg_cache_hit_pct,
    -- Composite efficiency score (0-100)
    ROUND(
        (AVG(avg_cpu_pct) / 100.0) * 25
        + LEAST(AVG(total_queries * 1.0 / NULLIF(dbu_consumed, 0)) / 200.0, 1.0) * 25
        + (AVG(cache_hits * 100.0 / NULLIF(total_queries, 0)) / 100.0) * 25
        + (AVG(uptime_hours) / 12.0) * 25,
        1
    )                                              AS efficiency_score
FROM warehouse_metrics
GROUP BY warehouse_id
ORDER BY efficiency_score DESC;
```

______________________________________________________________________

## :material-information-outline: Key Concepts

| Metric              | Formula                        | Optimal                      |
| ------------------- | ------------------------------ | ---------------------------- |
| **CPU Utilization** | `avg_cpu / max_capacity`       | 60–80% (headroom for spikes) |
| **DBU per Hour**    | `dbu_consumed / uptime_hours`  | Lower = more efficient       |
| **Queries per DBU** | `total_queries / dbu_consumed` | Higher = better throughput   |
| **Cache Hit Ratio** | `cache_hits / total_queries`   | >90% for BI workloads        |
| **Cost per Query**  | `dbu × price / queries`        | Lower = better value         |

!!! tip "Right-sizing signals"

    - CPU < 30% consistently → downsize warehouse
    - CPU > 85% with queue waits → upsize or add clusters
    - Cache hit < 50% → review query patterns, enable result caching

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario               | Key Metric                             |
| ---------------------- | -------------------------------------- |
| Warehouse right-sizing | CPU utilization + queries per DBU      |
| Cost optimisation      | Cost per query + idle time ratio       |
| Performance tuning     | Cache hit ratio + query duration       |
| Executive reporting    | Efficiency scorecard across warehouses |

______________________________________________________________________

## :material-database-search: [Databricks] Real-World Example — System Tables

!!! note "[Databricks] Unity Catalog system tables"

    `system.compute.node_timeline` and `system.billing.usage` are built-in
    Unity Catalog system tables (no sample data setup needed) that expose
    real utilization and billable usage. An account admin must grant
    `USE CATALOG` on `system`, `USE SCHEMA` on `system.compute` and
    `system.billing`, and `SELECT` on both tables before these queries will
    return rows.

### CPU efficiency per billed usage unit

```sql
-- [Databricks] Requires SELECT on system.compute.node_timeline and system.billing.usage
WITH daily_util AS (
    SELECT
        cluster_id,
        DATE(start_time) AS metric_date,
        ROUND(AVG(cpu_user_percent + cpu_system_percent), 1) AS avg_cpu_pct,
        ROUND(AVG(mem_used_percent), 1) AS avg_mem_pct
    FROM system.compute.node_timeline
    WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 14 DAYS
    GROUP BY cluster_id, DATE(start_time)
),
daily_usage AS (
    SELECT
        usage_metadata.cluster_id AS cluster_id,
        usage_date AS metric_date,
        ROUND(SUM(usage_quantity), 2) AS daily_usage_quantity,
        MIN(usage_unit) AS usage_unit
    FROM system.billing.usage
    WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 14)
      AND usage_metadata.cluster_id IS NOT NULL
    GROUP BY usage_metadata.cluster_id, usage_date
)
SELECT
    u.cluster_id,
    u.metric_date,
    u.avg_cpu_pct,
    u.avg_mem_pct,
    b.daily_usage_quantity,
    b.usage_unit,
    ROUND(u.avg_cpu_pct / NULLIF(b.daily_usage_quantity, 0), 2) AS cpu_pct_per_usage_unit,
    ROUND(u.avg_mem_pct / NULLIF(b.daily_usage_quantity, 0), 2) AS mem_pct_per_usage_unit
FROM daily_util AS u
JOIN daily_usage AS b
    ON u.cluster_id = b.cluster_id
    AND u.metric_date = b.metric_date
ORDER BY cpu_pct_per_usage_unit DESC, u.metric_date DESC;
-- Result (illustrative):
-- cluster_id        | metric_date | avg_cpu_pct | avg_mem_pct | daily_usage_quantity | usage_unit | cpu_pct_per_usage_unit | mem_pct_per_usage_unit
-- ----------------- | ----------- | ----------- | ----------- | -------------------- | ---------- | ---------------------- | ----------------------
-- 0315-1015-abcd123 | 2024-07-02  | 74.2        | 66.8        | 18.40                | DBU        | 4.03                   | 3.63
-- 0315-2045-efgh456 | 2024-07-02  | 29.5        | 34.2        | 16.90                | DBU        | 1.75                   | 2.02
```

!!! tip "Efficiency is still output divided by input"

    The metric changes from toy throughput counts to real billed usage, but
    the same ratio-based logic holds: join a useful-work signal to a cost or
    capacity input, then compare resources on a common efficiency scale.

______________________________________________________________________

## :material-arrow-right: Related

- [Cost Attribution](cost-attribution.md) — allocate costs to users and queries
- [Workload Classification](workload-classification.md) — classify by query type
- [Utilization Analysis](utilization-analysis.md) — busy/idle time breakdown
- [Idle Time Analysis](idle-time-analysis.md) — measure wasted compute
