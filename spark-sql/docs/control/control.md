# :material-code-braces: Control Flow in Spark SQL

Control flow constructs such as `CASE`, `IF`, and `COALESCE` allow conditional
logic directly inside SQL queries.

---

## 📌 Common Constructs

| Construct | Purpose |
|-----------|---------|
| `CASE WHEN` | Multi-branch conditional |
| `IF` | Single boolean conditional |
| `COALESCE` | First non-NULL value |
| `NULLIF` | Return NULL if two values match |

---

## 🧪 Practical Examples

### CASE WHEN

```sql
SELECT order_id,
       CASE
         WHEN amount > 1000 THEN 'high'
         WHEN amount > 100 THEN 'medium'
         ELSE 'low'
       END AS order_band
FROM orders;
```

### IF

```sql
SELECT order_id, IF(status = 'shipped', 1, 0) AS shipped_flag
FROM orders;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Derived categories | `CASE WHEN` |
| Binary flag | `IF` |
| NULL fallback | `COALESCE` |
