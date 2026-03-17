# Regex Functions

Spark SQL regex functions support pattern matching and extraction.

---

## 📌 Common Functions

| Function | Purpose |
|----------|---------|
| `REGEXP_LIKE` / `RLIKE` | Boolean match |
| `REGEXP_EXTRACT` | Extract capture group |
| `REGEXP_REPLACE` | Replace matches |
| `REGEXP_COUNT` | Count matches |
| `REGEXP_INSTR` | Position of match |
| `REGEXP_SUBSTR` | Return matched substring |

---

## 🧪 Example

```sql
SELECT REGEXP_EXTRACT('abc-123', '([a-z]+)-(\d+)', 2) AS num;
```

---

## 🧠 When to Use

| Scenario | Function |
|----------|----------|
| Validate strings | `REGEXP_LIKE` |
| Extract tokens | `REGEXP_EXTRACT` |
| Replace patterns | `REGEXP_REPLACE` |
