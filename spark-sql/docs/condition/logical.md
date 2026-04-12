# :material-logic-and: Logical Conditions

Logical operators combine or invert predicates to build more complex filters.

---

## 📌 Operators

| Operator | Example | Description |
|----------|---------|-------------|
| `AND` | `A AND B` | True when both are true |
| `OR` | `A OR B` | True when either is true |
| `NOT` | `NOT A` | Inverts a condition |

---

## 🔍 Precedence

`NOT` is evaluated first, then `AND`, then `OR`.
Use parentheses to make precedence explicit.

---

## 🧪 Practical Examples

### Combine Conditions

```sql
SELECT * FROM orders
WHERE status = 'shipped' AND amount > 100;
```

### Use Parentheses for Clarity

```sql
SELECT * FROM users
WHERE (country = 'US' OR country = 'CA')
  AND is_active = true;
```

### Exclude a Condition

```sql
SELECT * FROM events
WHERE NOT (event_type = 'test');
```

---

## 🧠 When to Use

| Scenario | Pattern |
|----------|---------|
| Combine requirements | `AND` |
| Allow alternatives | `OR` |
| Exclude matches | `NOT` |
| Avoid ambiguity | Parentheses |

---

> **Tip:** Prefer explicit parentheses when mixing `AND` and `OR` to avoid
> logical mistakes.
