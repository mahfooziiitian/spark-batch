# :material-lock-clock: Resource Contention

Identify **multiple jobs competing for resources** — detect warehouse concurrency
conflicts, lock contention, and resource saturation.

---

## :material-sitemap: Detection Flow

```mermaid
flowchart LR
    JOBS[Job Execution Logs\nstart · end · resource] --> OVERLAP[Concurrency Calculation\nrunning sum of ±1]
    OVERLAP --> PEAK[Peak Contention\nMAX concurrent]
    PEAK --> WAIT[Wait Analysis\nqueue time from overlap]

    style JOBS fill:#e3f2fd,stroke:#1e88e5
    style OVERLAP fill:#e8f5e9,stroke:#43a047
    style WAIT fill:#fce4ec,stroke:#e53935
```

---

## :material-code-tags: Syntax

### Sample data

```sql
CREATE OR REPLACE TEMP VIEW job_executions AS
SELECT * FROM VALUES
  (1, 'warehouse_1', 'etl_daily',   TIMESTAMP '2024-03-01 08:00', TIMESTAMP '2024-03-01 08:45'),
  (2, 'warehouse_1', 'bi_report',   TIMESTAMP '2024-03-01 08:10', TIMESTAMP '2024-03-01 08:30'),
  (3, 'warehouse_1', 'ml_training', TIMESTAMP '2024-03-01 08:20', TIMESTAMP '2024-03-01 09:15'),
  (4, 'warehouse_1', 'ad_hoc_1',    TIMESTAMP '2024-03-01 08:25', TIMESTAMP '2024-03-01 08:35'),
  (5, 'warehouse_1', 'ad_hoc_2',    TIMESTAMP '2024-03-01 09:00', TIMESTAMP '2024-03-01 09:10'),
  (6, 'warehouse_2', 'etl_hourly',  TIMESTAMP '2024-03-01 08:00', TIMESTAMP '2024-03-01 08:20'),
  (7, 'warehouse_2', 'dashboard',   TIMESTAMP '2024-03-01 08:05', TIMESTAMP '2024-03-01 08:25')
AS t(job_id, warehouse_id, job_name, start_time, end_time);
```

---

### Concurrency at each moment

```sql
WITH boundaries AS (
    SELECT warehouse_id, start_time AS ts, 1 AS delta FROM job_executions
    UNION ALL
    SELECT warehouse_id, end_time, -1 FROM job_executions
)
SELECT
    warehouse_id,
    ts,
    SUM(delta) OVER (
        PARTITION BY warehouse_id ORDER BY ts
        ROWS UNBOUNDED PRECEDING
    )                                              AS concurrent_jobs
FROM boundaries
ORDER BY warehouse_id, ts;
```

---

### Peak concurrency per warehouse

```sql
WITH boundaries AS (
    SELECT warehouse_id, start_time AS ts, 1 AS delta FROM job_executions
    UNION ALL
    SELECT warehouse_id, end_time, -1 FROM job_executions
),
running AS (
    SELECT
        warehouse_id,
        ts,
        SUM(delta) OVER (
            PARTITION BY warehouse_id ORDER BY ts ROWS UNBOUNDED PRECEDING
        ) AS concurrent
    FROM boundaries
)
SELECT
    warehouse_id,
    MAX(concurrent)                                AS peak_concurrency,
    MIN(ts)                                        AS peak_time
FROM running
WHERE concurrent = (SELECT MAX(concurrent) FROM running r2 WHERE r2.warehouse_id = running.warehouse_id)
GROUP BY warehouse_id;
```

---

### Contention periods (>= N concurrent)

```sql
WITH boundaries AS (
    SELECT warehouse_id, start_time AS ts, 1 AS delta, job_name FROM job_executions
    UNION ALL
    SELECT warehouse_id, end_time, -1, job_name FROM job_executions
),
running AS (
    SELECT
        warehouse_id, ts,
        SUM(delta) OVER (PARTITION BY warehouse_id ORDER BY ts ROWS UNBOUNDED PRECEDING)
            AS concurrent
    FROM boundaries
)
SELECT
    warehouse_id,
    ts                                             AS contention_time,
    concurrent
FROM running
WHERE concurrent >= 3
ORDER BY warehouse_id, ts;
```

---

### Job overlap matrix

```sql
SELECT
    a.job_name                                     AS job_a,
    b.job_name                                     AS job_b,
    a.warehouse_id,
    ROUND(
        (UNIX_TIMESTAMP(LEAST(a.end_time, b.end_time))
         - UNIX_TIMESTAMP(GREATEST(a.start_time, b.start_time))) / 60.0,
        1
    )                                              AS overlap_minutes
FROM job_executions a
JOIN job_executions b
    ON a.warehouse_id = b.warehouse_id
    AND a.job_id < b.job_id
    AND a.start_time < b.end_time
    AND b.start_time < a.end_time
ORDER BY overlap_minutes DESC;
```

---

## :material-information-outline: Key Concepts

| Metric | Detection | Impact |
|--------|-----------|--------|
| **Peak concurrency** | MAX of running sum | Resource saturation point |
| **Overlap duration** | `LEAST(end1,end2) - GREATEST(start1,start2)` | Contention severity |
| **Contention frequency** | Periods where concurrent > threshold | How often bottlenecks occur |
| **Wait time** | Queue time when resources exhausted | User-perceived latency |

---

## :material-lightbulb-outline: When to Use

| Scenario | Analysis |
|----------|----------|
| Databricks warehouse sizing | Peak concurrency vs cluster limit |
| ETL scheduling | Avoid overlapping heavy jobs |
| Database lock contention | Concurrent DML on same tables |
| CI/CD runner capacity | Jobs waiting for runners |

---

## :material-arrow-right: Related

- [Queue Analysis](queue_analysis.md) — wait and processing time measurement
- [Interval Analytics](interval_analytics.md) — overlap detection patterns
- [Utilization Analysis](utilization_analysis.md) — resource busy/idle time
