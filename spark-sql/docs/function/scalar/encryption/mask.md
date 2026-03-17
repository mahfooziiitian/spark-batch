# MASK

`MASK` obfuscates sensitive values by replacing characters with defaults.

---

## 📌 Syntax

```sql
MASK(str)
MASK(str, upper, lower, digit, other)
```

---

## 🔍 Behavior Notes

1. Letters and digits are replaced by default mask characters.
2. You can customize replacement characters for upper, lower, digits, and other.

---

## 🧪 Examples

```sql
SELECT MASK('John.Doe@example.com') AS masked;
```

```sql
SELECT MASK('ABC123', 'X', 'x', '0', '*') AS masked;
```

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Mask PII | Use `MASK` |
| Consistent obfuscation | Customize mask chars |
