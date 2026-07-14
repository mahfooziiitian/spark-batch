# :material-table: Tables

Tables are the core storage abstraction in Spark SQL. They can be managed or
external and use formats like Parquet, ORC, or Delta.

---

## :material-pin: Table Types

| Type | Description |
|------|-------------|
| Managed | Spark controls data and metadata |
| External | Data stored outside Spark's warehouse |

---

## :material-flask-outline: Example

```sql
CREATE TABLE orders (
  order_id BIGINT,
  amount DOUBLE
) USING PARQUET;
```

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Spark-managed lifecycle | Managed tables |
| Shared storage | External tables |
| ACID operations | Delta tables |
