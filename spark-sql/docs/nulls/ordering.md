# NULL Ordering

By default, Spark SQL sorts NULLs **first** in ascending order and **last** in
descending order. You can override this behavior explicitly.

---

## 📌 Syntax

```sql
ORDER BY col ASC NULLS FIRST
ORDER BY col ASC NULLS LAST
ORDER BY col DESC NULLS FIRST
ORDER BY col DESC NULLS LAST
```

---

## 🧪 Examples

```sql
SELECT * FROM users
ORDER BY last_login ASC NULLS LAST;
```

```sql
SELECT * FROM users
ORDER BY last_login DESC NULLS FIRST;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Show recent values first | `DESC NULLS LAST` |
| Push missing values down | `NULLS LAST` |
| Highlight missing values | `NULLS FIRST` |
