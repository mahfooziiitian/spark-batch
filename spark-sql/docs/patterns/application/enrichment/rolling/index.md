# :material-chart-bell-curve: Rolling Analysis

Running totals, moving averages, `LAG`/`LEAD` comparisons, and cumulative distributions
computed with window functions — the enrichment stage of a pipeline.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Ordered partition] --> B[Window frame]
    B --> C[ROWS BETWEEN ... AND ...]
    C --> D[Accumulated value per row]
```

______________________________________________________________________

## :material-map-marker-path: Canonical Deep-Dives

Most rolling-window techniques have a dedicated, self-contained page with full sample
data and result output. Use those as the source of truth; this page only adds the
distribution functions not covered elsewhere.

| Technique                                     | Canonical page                                                                  |
| --------------------------------------------- | ------------------------------------------------------------------------------- |
| Running / cumulative total                    | [Running Total](../../../aggregation/running-total.md)                          |
| Moving / rolling average                      | [Moving Average](../../../aggregation/moving-average.md)                        |
| `ROW_NUMBER` unique IDs & dedup               | [Top-N Per Group](../../../ranking/top-n.md)                                    |
| `LAG` / `LEAD` period comparison              | [LAG & LEAD](../../../timeseries/analysis/lag-and-lead.md)                      |
| Missing-date spine (`SEQUENCE` + `LEFT JOIN`) | [Gap Fill](../../../timeseries/analysis/gap-filling.md)                         |
| `FIRST_VALUE` / `LAST_VALUE` boundary fill    | [Gap Fill — forward/backward fill](../../../timeseries/analysis/gap-filling.md) |

______________________________________________________________________

## :material-magnify: Distribution Ranking (unique to this page)

### CUME_DIST and PERCENT_RANK

Calculate the cumulative distribution and relative rank of each row within an ordered
partition — useful for "top X%" thresholds and percentile banding.

```sql
--8<-- "sql/application/rolling/08_cume_dist_percent_rank.sql"
```

- `CUME_DIST()` — fraction of rows with a value **≤** the current row (0 < d ≤ 1).
- `PERCENT_RANK()` — relative rank as `(rank - 1) / (rows - 1)` (0 ≤ r ≤ 1).

______________________________________________________________________

## :material-database-search: [Databricks] Real-World Example — System Tables

!!! note "[Databricks] Unity Catalog system tables"

    `system.billing.usage` is a built-in Unity Catalog system table containing real
    account consumption records. An account admin must `GRANT USE CATALOG, USE SCHEMA, SELECT ON SCHEMA system.billing TO <principal>` before these queries will return
    rows.

### Rolling 7-day average of billed usage per workspace

```sql
-- [Databricks] Requires SELECT on system.billing.usage
WITH daily_usage AS (
    SELECT
        workspace_id,
        usage_date,
        SUM(usage_quantity) AS daily_quantity
    FROM system.billing.usage
    WHERE usage_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY workspace_id, usage_date
)
SELECT
    workspace_id,
    usage_date,
    daily_quantity,
    ROUND(
        AVG(daily_quantity) OVER (
            PARTITION BY workspace_id
            ORDER BY usage_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS rolling_7d_avg,
    ROUND(
        SUM(daily_quantity) OVER (
            PARTITION BY workspace_id
            ORDER BY usage_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        2
    ) AS cumulative_quantity
FROM daily_usage
ORDER BY workspace_id, usage_date;
-- Result (illustrative):
-- workspace_id | usage_date  | daily_quantity | rolling_7d_avg | cumulative_quantity
-- ------------|-------------|----------------|----------------|--------------------
-- 123456789   | 2024-07-01  | 168.40         | 168.40         | 168.40
-- 123456789   | 2024-07-02  | 171.10         | 169.75         | 339.50
-- 123456789   | 2024-07-07  | 190.80         | 178.96         | 1249.60
```

!!! tip "Same window frame, real billing data"

    Once you aggregate to one row per day, rolling windows on `system.billing.usage`
    behave exactly like the teaching examples. The only difference is that the source
    rows now represent real workspace consumption instead of synthetic measurements.

______________________________________________________________________

## :material-brain: When to Use

| Scenario                       | Recommended Approach                                       |
| ------------------------------ | ---------------------------------------------------------- |
| Cumulative totals              | [Running Total](../../../aggregation/running-total.md)     |
| Moving average                 | [Moving Average](../../../aggregation/moving-average.md)   |
| Period-over-period comparison  | [LAG & LEAD](../../../timeseries/analysis/lag-and-lead.md) |
| Gap detection / date spine     | [Gap Fill](../../../timeseries/analysis/gap-filling.md)    |
| "Top X%" / percentile position | `CUME_DIST` / `PERCENT_RANK` (above)                       |

!!! warning

    `LAST_VALUE()` requires `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
    (or an explicit frame) — the default frame stops at `CURRENT ROW`.
