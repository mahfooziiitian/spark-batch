# :material-key: Keys

Keys uniquely identify rows and define relationships between tables.

---

## :material-pin: Common Key Types

| Key Type | Description |
|----------|-------------|
| Primary key | Unique identifier for a row |
| Foreign key | References another table's key |
| Surrogate key | System-generated identifier |
| Composite key | Combination of columns |

---

## :material-flask-outline: Example

```sql
CREATE TABLE customers (
  customer_id BIGINT,
  name STRING
);
```

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Uniquely identify rows | Primary key |
| Data warehouse modeling | Surrogate keys |
| Natural relationships | Foreign keys |
