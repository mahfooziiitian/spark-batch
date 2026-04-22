# :material-math-integral: SQL Operators

Operators combine values and expressions. Spark SQL supports arithmetic,
comparison, logical, and string operators.

---

## :material-pin: Operator Categories

| Category | Examples |
|----------|----------|
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| Comparison | `=`, `!=`, `>`, `<`, `>=`, `<=`, `<=>` |
| Logical | `AND`, `OR`, `NOT` |
| String | `||` (concat) |
| Set | `IN`, `BETWEEN` |

---

## :material-flask-outline: Examples

```sql
SELECT 10 + 5 AS sum,
       10 % 3 AS remainder,
       'ab' || 'cd' AS concat;
```

```sql
SELECT * FROM users
WHERE age >= 18 AND country = 'US';
```

---

## :material-brain: When to Use

| Scenario | Operator |
|----------|----------|
| Combine numeric values | Arithmetic |
| Compare values | Comparison |
| Filter with logic | Logical |
| Build strings | `||` concat |
