# Pattern Matching Conditions

Pattern predicates filter strings based on partial matches or regular
expressions.

---

## 📌 Operators

| Operator | Example | Description |
|----------|---------|-------------|
| `LIKE` | `name LIKE 'A%'` | `%` = many chars, `_` = one char |
| `RLIKE` | `name RLIKE '^A.*'` | Regex match |
| `NOT LIKE` | `name NOT LIKE '%test%'` | Negated pattern |

---

## 🔍 Behavior Notes

1. `LIKE` is case-sensitive by default.
2. Use `ESCAPE` to match literal `%` or `_` characters.
3. `RLIKE` uses Java regex syntax.

---

## 🧪 Practical Examples

### Prefix Match

```sql
SELECT * FROM users
WHERE name LIKE 'A%';
```

### Suffix Match

```sql
SELECT * FROM files
WHERE filename LIKE '%.csv';
```

### Escape Wildcards

```sql
SELECT * FROM logs
WHERE message LIKE '%\_%' ESCAPE '\';
```

### Regex Match

```sql
SELECT * FROM events
WHERE event_id RLIKE '^[A-Z]{3}-[0-9]{4}$';
```

---

## 🧠 When to Use

| Scenario | Pattern |
|----------|---------|
| Simple wildcard matching | `LIKE` |
| Complex regex | `RLIKE` |
| Literal `%` or `_` | `ESCAPE` with `LIKE` |

---

> **Tip:** Prefer exact matches when possible; regex can be expensive.
