# :material-server: Physical Planning

Physical planning selects concrete execution operators for a logical plan.

### :material-sitemap: Overview

```mermaid
graph TD
    A[Optimized Logical Plan] --> B[Physical Planning]
    B --> C{Strategy}
    C -->|Small table| D[Broadcast Join]
    C -->|Large table| E[Sort-Merge Join]
    C -->|Low cardinality| F[Hash Aggregation]
```

---

## 📌 What It Chooses

| Choice | Examples |
|--------|----------|
| Join strategy | Broadcast, sort-merge |
| Aggregation | Hash agg, sort agg |
| Scan | Parquet/ORC/Delta scan |

---

## 🧪 Example

```sql
EXPLAIN FORMATTED SELECT * FROM orders JOIN customers USING (id);
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Validate join type | Check physical plan |
| Tuning | Compare strategies |
