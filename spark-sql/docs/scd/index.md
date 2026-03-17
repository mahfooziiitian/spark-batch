# Slowly Changing Dimensions (SCD)

SCD patterns track changes in dimension tables over time. Spark SQL supports
multiple SCD types depending on how history should be preserved.

---

## 📌 SCD Types

| Type | Description |
|------|-------------|
| Type 1 | Overwrite old values |
| Type 2 | Add new rows for history |
| Type 3 | Store limited history in additional columns |
| Type 4 | Separate history table |
| Type 5 | Hybrid of 1 and 4 |
| Type 6 | Hybrid of 1, 2, and 3 |

---

## 🧪 Example (Type 2)

```sql
INSERT INTO dim_customer
SELECT id, name, current_date() AS effective_from,
       NULL AS effective_to, true AS is_current
FROM staging_customer;
```

---

## 🧠 When to Use

| Scenario | SCD Type |
|----------|----------|
| No history needed | Type 1 |
| Full history | Type 2 |
| Limited history | Type 3 |
| Separate history table | Type 4 |
