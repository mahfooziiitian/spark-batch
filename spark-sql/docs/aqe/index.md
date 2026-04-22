# :material-lightning-bolt: Adaptive Query Execution (AQE)

AQE adjusts query plans at runtime based on actual data statistics.
It can optimize join strategies, shuffle partitions, and skew handling.

### :material-sitemap: Overview

```mermaid
graph TD
    A[Query Submitted] --> B[Initial Plan]
    B --> C{AQE Enabled?}
    C -->|Yes| D[Collect Runtime Stats]
    D --> E[Re-optimize Plan]
    E --> F[Coalesce Partitions?]
    E --> G[Change Join Strategy?]
    E --> H[Handle Skew?]
    F --> I[Execute]
    G --> I
    H --> I
    C -->|No| I
```

---

## :material-pin: Key Features

| Feature | Benefit |
|---------|---------|
| Join strategy changes | Switch to broadcast if smaller than expected |
| Shuffle coalescing | Reduce small partitions |
| Skew handling | Split skewed partitions |

---

## :material-flask-outline: Example

```sql
SET spark.sql.adaptive.enabled = true;
SELECT * FROM big_table JOIN small_dim USING (id);
```

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Variable data sizes | Enable AQE |
| Skewed joins | AQE skew handling |
| Too many small partitions | AQE coalescing |
