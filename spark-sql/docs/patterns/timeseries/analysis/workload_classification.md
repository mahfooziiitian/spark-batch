# :material-format-list-group: Workload Classification

Classify workloads by **type (Interactive, Batch, ETL, BI, ML)** — using query
characteristics, duration patterns, and resource consumption for capacity planning.

---

## :material-sitemap: Classification Flow

```mermaid
flowchart LR
    QUERIES[Query History] --> FEATURES[Feature Extraction\nDuration · Rows · Frequency]
    FEATURES --> RULES[Classification Rules\nThresholds · Patterns]
    RULES --> LABELS[Workload Labels\nInteractive · Batch · ETL · BI · ML]

    style QUERIES fill:#e3f2fd,stroke:#1e88e5
    style FEATURES fill:#e8f5e9,stroke:#43a047
    style LABELS fill:#fce4ec,stroke:#e53935
```

---

## :material-code-tags: Syntax

### Sample data

```sql
CREATE OR REPLACE TEMP VIEW query_history AS
SELECT * FROM VALUES
  (1,  'user_1', 'SELECT * FROM sales LIMIT 100',          5,    1000,     0.1),
  (2,  'user_1', 'SELECT COUNT(*) FROM events',            3,    1,        0.05),
  (3,  'user_2', 'INSERT OVERWRITE TABLE daily_agg ...',   300,  5000000,  45.0),
  (4,  'user_2', 'MERGE INTO dim_customer USING ...',      180,  200000,   30.0),
  (5,  'user_3', 'SELECT region, SUM(rev) GROUP BY ...',   15,   50,       2.0),
  (6,  'user_3', 'SELECT * FROM dashboard_cache',          2,    200,      0.02),
  (7,  'user_4', 'CREATE TABLE ml_features AS SELECT ...',  600,  10000000, 120.0),
  (8,  'user_4', 'SELECT * FROM model_predictions',        8,    5000,     1.0),
  (9,  'svc_etl','INSERT INTO fact_orders SELECT ...',     450,  8000000,  80.0),
  (10, 'svc_bi', 'SELECT * FROM exec_dashboard',           4,    100,      0.1)
AS t(query_id, user_id, query_text, duration_sec, rows_produced, dbu_cost);
```

---

### Rule-based classification

```sql
SELECT
    query_id,
    user_id,
    duration_sec,
    rows_produced,
    dbu_cost,
    CASE
        WHEN user_id LIKE 'svc_etl%'
             OR query_text LIKE '%INSERT%OVERWRITE%'
             OR query_text LIKE '%MERGE%INTO%'
            THEN 'ETL'
        WHEN user_id LIKE 'svc_bi%'
             OR query_text LIKE '%dashboard%'
            THEN 'BI'
        WHEN duration_sec > 500
             OR query_text LIKE '%ml_%'
             OR query_text LIKE '%model_%'
            THEN 'ML'
        WHEN duration_sec > 120 AND rows_produced > 100000
            THEN 'BATCH'
        ELSE 'INTERACTIVE'
    END                                            AS workload_type
FROM query_history
ORDER BY workload_type, duration_sec DESC;
```

---

### Workload summary statistics

```sql
WITH classified AS (
    SELECT *,
        CASE
            WHEN user_id LIKE 'svc_etl%' OR query_text LIKE '%INSERT%OVERWRITE%'
                 OR query_text LIKE '%MERGE%INTO%' THEN 'ETL'
            WHEN user_id LIKE 'svc_bi%' OR query_text LIKE '%dashboard%' THEN 'BI'
            WHEN duration_sec > 500 OR query_text LIKE '%ml_%' THEN 'ML'
            WHEN duration_sec > 120 AND rows_produced > 100000 THEN 'BATCH'
            ELSE 'INTERACTIVE'
        END AS workload_type
    FROM query_history
)
SELECT
    workload_type,
    COUNT(*)                                       AS query_count,
    ROUND(AVG(duration_sec), 1)                    AS avg_duration_sec,
    ROUND(SUM(dbu_cost), 2)                        AS total_dbu,
    ROUND(SUM(dbu_cost) * 100.0 / (SELECT SUM(dbu_cost) FROM classified), 1)
                                                   AS cost_pct
FROM classified
GROUP BY workload_type
ORDER BY total_dbu DESC;
```

---

## :material-information-outline: Key Concepts

| Workload Type | Characteristics | Typical Duration |
|---------------|-----------------|-----------------|
| **Interactive** | Low latency, small results, ad-hoc | < 30 sec |
| **BI** | Dashboard queries, cached, frequent | < 15 sec |
| **ETL** | INSERT/MERGE, high row count, scheduled | 2–30 min |
| **Batch** | Large scans, aggregations, periodic | 2–10 min |
| **ML** | Feature generation, model training, massive | 10+ min |

---

## :material-lightbulb-outline: When to Use

| Scenario | Action |
|----------|--------|
| Warehouse sizing | Size for peak of each workload type separately |
| Cost allocation | Charge teams by workload type cost |
| Scheduling | Separate ETL from interactive windows |
| Performance SLA | Different latency targets per type |

---

## :material-arrow-right: Related

- [Cost Attribution](cost_attribution.md) — allocate costs to users and queries
- [Resource Contention](resource_contention.md) — detect competing workloads
- [Resource Efficiency](resource_efficiency.md) — measure per-query efficiency
