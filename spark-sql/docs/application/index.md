# Applications

This section focuses on practical Spark SQL solutions used in real pipelines.
It includes patterns for de-duplication, key replacement, and data quality
workflows.

---

## 📌 Topics

| Topic | Description |
|-------|-------------|
| Duplicate handling | Identify and remove duplicates |
| Key replacement | Substitute or remap keys |
| Data cleanup | Normalize and standardize datasets |

---

## 🧪 Example: De-duplicate with Window

```sql
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
  FROM source
)
SELECT * FROM ranked WHERE rn = 1;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Data cleanup pipelines | Use application patterns |
| Standardize keys | Apply key replacement logic |
| Data quality enforcement | De-duplication and validation |
