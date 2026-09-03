# :material-shield-check: Data Quality

Detect, quantify, and monitor data integrity problems — value drift, statistical
outliers, snapshot differences, and fraud signals — so bad data is caught before it
reaches reports and models.

---

## :material-sitemap: Detection Flow

```mermaid
flowchart LR
    IN["Incoming / historical data"] --> CMP["Compare\nvs previous rows or snapshots"]
    IN --> STAT["Score\nz-score, IQR, percentiles"]
    CMP --> FLAG["Flag changes & anomalies"]
    STAT --> FLAG
    FLAG --> ALERT["Alert / audit / quarantine"]
```

---

## :material-view-grid: In This Section

| Page | Problem | Key Technique |
|------|---------|---------------|
| [Change Detection](change_detection.md) | Spot when a value changes between consecutive rows | `LAG`, state-transition flags |
| [Outlier Detection](outlier_detection.md) | Find points that deviate from the norm | z-score, IQR fencing, percentile thresholds |
| [Slowly Changing Comparison](slowly_changing_comparison.md) | Diff current vs previous snapshots | full-outer snapshot join, column-level diff |
| [Fraud Pattern Detection](fraud_detection.md) | Surface suspicious multi-entity patterns | self-joins, `COUNT(DISTINCT)`, impossible-travel checks |

---

## :material-lightbulb-outline: When to Use

- **Monitoring** — continuously watch production tables for drift and anomalies.
- **Audit trails** — record what changed, when, and by how much.
- **Incremental ETL validation** — verify each load against the prior snapshot.
- **Risk & fraud** — flag entities whose behaviour breaks expected constraints.

---

!!! note "Related"
    For **duplicate finding and deduplication** (an ingestion-layer integrity task),
    see the applied [Data Quality](../application/data_quality/index.md) walkthroughs.
