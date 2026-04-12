# :material-lightning-bolt: Caching Overview

Caching stores intermediate results in memory to speed up repeated queries.
It is most useful for iterative analysis or multi-step pipelines.

### :material-sitemap: Overview

```mermaid
graph LR
    A[First Query] --> B[CACHE TABLE]
    B --> C[In-Memory Store]
    C --> D[Subsequent Queries]
    D -->|Cache hit| C
    D -->|Cache miss| E[Recompute]
```

---

## 📌 Commands

```sql
CACHE TABLE orders;
UNCACHE TABLE orders;
```

---

## 🔍 Behavior Notes

1. Cached data consumes memory; evictions happen under pressure.
2. Use caching for datasets reused across multiple queries.
3. Prefer `CACHE TABLE` for SQL workflows.

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Repeated queries | Cache results |
| One-time query | Skip caching |
| Large datasets | Cache selectively |
