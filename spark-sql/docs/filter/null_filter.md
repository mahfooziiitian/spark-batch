# :material-null: NULL Handling in Filters

Spark SQL uses **three-valued logic**: `TRUE`, `FALSE`, and `NULL`.
That means comparisons involving NULL often return NULL (which behaves like
FALSE in filters). Understanding this behavior avoids surprising results.

---

## 📌 Key Rules

1. `NULL` is not equal to anything — even another `NULL`.
2. Comparisons like `col != 'x'` return NULL when `col` is NULL.
3. Filters keep only rows where the condition is **TRUE**.

---

## 🔍 Common Patterns

### 1) Explicit NULL Checks

```sql
SELECT * FROM users WHERE last_login IS NULL;
SELECT * FROM users WHERE last_login IS NOT NULL;
```

### 2) NULL-Safe Equality (`<=>`)

```sql
SELECT * FROM users WHERE last_login <=> NULL;   -- matches NULLs
SELECT * FROM users WHERE region <=> 'US';       -- handles NULL safely
```

### 3) Replace NULLs with Defaults

```sql
SELECT * FROM orders
WHERE COALESCE(amount, 0) > 100;
```

### 4) Preserve NULLs in Inequality Filters

```sql
SELECT * FROM products
WHERE price != 0 OR price IS NULL;
```

---

## Truth Table (Simplified)

| Expression | Result |
|------------|--------|
| `NULL = 'x'` | NULL |
| `NULL != 'x'` | NULL |
| `NULL <=> NULL` | TRUE |
| `NULL IS NULL` | TRUE |
| `NULL IS NOT NULL` | FALSE |

---

## 🧪 Practical Example

```sql
WITH demo AS (
  SELECT * FROM VALUES
    (1, 'Alice', NULL),
    (2, 'Bob', 'US'),
    (3, 'Cara', NULL)
  AS demo(id, name, region)
)
SELECT * FROM demo
WHERE region IS NULL OR region = 'US';
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Find missing values | `IS NULL` |
| Compare with NULL safely | `<=>` |
| Treat NULL as default | `COALESCE(col, default)` |
| Keep NULLs in inequality | Add `OR col IS NULL` |

---

> **Tip:** If you see fewer rows than expected, check for NULLs in your filter
> columns first.
