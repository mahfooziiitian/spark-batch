# Keys

Keys uniquely identify rows and define relationships between tables.

---

## 📌 Common Key Types

| Key Type | Description |
|----------|-------------|
| Primary key | Unique identifier for a row |
| Foreign key | References another table's key |
| Surrogate key | System-generated identifier |
| Composite key | Combination of columns |

---

## 🧪 Example

```sql
CREATE TABLE customers (
  customer_id BIGINT,
  name STRING
);
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Uniquely identify rows | Primary key |
| Data warehouse modeling | Surrogate keys |
| Natural relationships | Foreign keys |
