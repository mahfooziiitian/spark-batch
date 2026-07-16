# :material-currency-usd: Cost Attribution

Attribute **costs to queries, warehouses, users, and workspaces** — track Databricks
spend by entity for chargeback, budgeting, and optimisation. **[Databricks]**

---

## :material-sitemap: Attribution Flow

```mermaid
flowchart LR
    USAGE[Usage Logs\nDBU · duration · bytes] --> QUERY[Per-Query Cost\nDBU × price]
    QUERY --> USER[Per-User Rollup\nSUM by user]
    USER --> WH[Per-Warehouse\nSUM by warehouse]
    WH --> WS[Per-Workspace\nTotal allocation]

    style USAGE fill:#e3f2fd,stroke:#1e88e5
    style QUERY fill:#e8f5e9,stroke:#43a047
    style WH fill:#fff3e0,stroke:#fb8c00
    style WS fill:#fce4ec,stroke:#e53935
```

---

## :material-code-tags: Syntax

### Sample data

```sql
CREATE OR REPLACE TEMP VIEW query_usage AS
SELECT * FROM VALUES
  (1,  'wh_prod',  'ws_analytics', 'user_1', 0.5,   30,  DATE '2024-03-01'),
  (2,  'wh_prod',  'ws_analytics', 'user_1', 2.0,   180, DATE '2024-03-01'),
  (3,  'wh_prod',  'ws_analytics', 'user_2', 0.1,   5,   DATE '2024-03-01'),
  (4,  'wh_dev',   'ws_dev',       'user_3', 5.0,   600, DATE '2024-03-01'),
  (5,  'wh_dev',   'ws_dev',       'user_3', 3.0,   300, DATE '2024-03-01'),
  (6,  'wh_prod',  'ws_analytics', 'user_2', 1.5,   120, DATE '2024-03-01'),
  (7,  'wh_bi',    'ws_bi',        'svc_bi', 0.05,  3,   DATE '2024-03-01'),
  (8,  'wh_bi',    'ws_bi',        'svc_bi', 0.05,  2,   DATE '2024-03-01'),
  (9,  'wh_bi',    'ws_bi',        'svc_bi', 0.05,  4,   DATE '2024-03-01'),
  (10, 'wh_prod',  'ws_analytics', 'user_1', 10.0,  900, DATE '2024-03-02')
AS t(query_id, warehouse_id, workspace_id, user_id, dbu_consumed, duration_sec, query_date);
```

---

### Cost per query

```sql
SELECT
    query_id,
    warehouse_id,
    user_id,
    dbu_consumed,
    duration_sec,
    -- Assuming $0.22 per DBU (serverless SQL)
    ROUND(dbu_consumed * 0.22, 4)                  AS query_cost_usd
FROM query_usage
ORDER BY query_cost_usd DESC;
```

---

### Cost per user

```sql
SELECT
    user_id,
    COUNT(*)                                       AS queries,
    ROUND(SUM(dbu_consumed), 2)                    AS total_dbu,
    ROUND(SUM(dbu_consumed) * 0.22, 2)             AS total_cost_usd,
    ROUND(AVG(dbu_consumed) * 0.22, 4)             AS avg_cost_per_query,
    ROUND(
        SUM(dbu_consumed) * 100.0 / (SELECT SUM(dbu_consumed) FROM query_usage),
        1
    )                                              AS cost_share_pct
FROM query_usage
GROUP BY user_id
ORDER BY total_cost_usd DESC;
```

---

### Cost per warehouse

```sql
SELECT
    warehouse_id,
    COUNT(*)                                       AS queries,
    COUNT(DISTINCT user_id)                        AS users,
    ROUND(SUM(dbu_consumed), 2)                    AS total_dbu,
    ROUND(SUM(dbu_consumed) * 0.22, 2)             AS total_cost_usd,
    ROUND(SUM(duration_sec) / 3600.0, 2)           AS total_hours
FROM query_usage
GROUP BY warehouse_id
ORDER BY total_cost_usd DESC;
```

---

### Cost per workspace

```sql
SELECT
    workspace_id,
    COUNT(DISTINCT warehouse_id)                   AS warehouses,
    COUNT(DISTINCT user_id)                        AS users,
    COUNT(*)                                       AS queries,
    ROUND(SUM(dbu_consumed) * 0.22, 2)             AS total_cost_usd,
    ROUND(AVG(dbu_consumed) * 0.22, 4)             AS avg_query_cost
FROM query_usage
GROUP BY workspace_id
ORDER BY total_cost_usd DESC;
```

---

### Daily cost trend

```sql
SELECT
    query_date,
    warehouse_id,
    COUNT(*)                                       AS queries,
    ROUND(SUM(dbu_consumed) * 0.22, 2)             AS daily_cost,
    LAG(ROUND(SUM(dbu_consumed) * 0.22, 2)) OVER (
        PARTITION BY warehouse_id ORDER BY query_date
    )                                              AS prev_day_cost
FROM query_usage
GROUP BY query_date, warehouse_id
ORDER BY warehouse_id, query_date;
```

---

## :material-information-outline: Key Concepts

| Dimension | Aggregation | Use Case |
|-----------|-------------|----------|
| **Per query** | Single row cost | Identify expensive queries |
| **Per user** | SUM by user | Chargeback / budget enforcement |
| **Per warehouse** | SUM by warehouse | Right-sizing decisions |
| **Per workspace** | SUM by workspace | Department allocation |
| **Per day** | SUM by date | Trend monitoring / anomaly alerts |

!!! tip "DBU pricing varies"
    Databricks pricing differs by SKU: Jobs Compute ($0.15), SQL Serverless ($0.22),
    All-Purpose ($0.40). Join with a pricing table for accurate attribution.

---

## :material-lightbulb-outline: When to Use

| Scenario | Analysis |
|----------|----------|
| Monthly chargeback | Per-user and per-workspace cost rollup |
| Budget alerts | Daily cost trend with threshold warnings |
| Query optimisation | Find top-10 expensive queries to tune |
| Warehouse right-sizing | Compare cost vs query count to find over-provisioned |
| Executive dashboard | Cost per workspace with MoM comparison |

---

## :material-arrow-right: Related

- [Workload Classification](workload_classification.md) — classify query types for allocation
- [Resource Efficiency](resource_efficiency.md) — cost per unit of work
- [Capacity Planning](capacity_planning.md) — forecast cost growth
