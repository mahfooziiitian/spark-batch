# REGEXP_EXTRACT

`REGEXP_EXTRACT` returns a capturing group from a string that matches a regex.

---

## 📌 Syntax

```sql
REGEXP_EXTRACT(str, pattern, idx)
```

---

## 🔍 Behavior

1. `idx` is the capture group index (0 = full match).
2. Returns empty string when there is no match.
3. Regex uses Java syntax.

---

## 🧪 Example

```sql
SELECT REGEXP_EXTRACT('abc-123', '([a-z]+)-(\d+)', 2) AS num;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Extract tokens | `REGEXP_EXTRACT` |
| Validate format | `REGEXP_LIKE` |
