# :material-call-split: IF, IIF, and Null-Handling Functions

Spark SQL provides compact conditional functions as alternatives to `CASE WHEN` for
simple two-branch or null-handling logic.

---

## :material-pin: Function Reference

| Function | Syntax | Returns | Notes |
|----------|--------|---------|-------|
| `IF` | `IF(cond, true_val, false_val)` | true_val or false_val | Spark-specific (not standard SQL) |
| `IIF` | `IIF(cond, true_val, false_val)` | true_val or false_val | Databricks alias for `IF` |
| `IFNULL` | `IFNULL(expr, replacement)` | expr if not NULL, else replacement | Equivalent to `COALESCE(expr, replacement)` |
| `NULLIF` | `NULLIF(expr, comparand)` | NULL if equal, else expr | Useful for divide-by-zero guards |
| `COALESCE` | `COALESCE(e1, e2, …)` | First non-NULL value | Standard SQL; accepts many args |
| `NVL` | `NVL(expr, replacement)` | expr if not NULL, else replacement | Spark alias for `IFNULL` |
| `NVL2` | `NVL2(expr, not_null_val, null_val)` | not_null_val if expr not NULL | Three-arg NULL switch |

---

## :material-table: IF vs CASE WHEN

| Aspect | `IF(…)` | `CASE WHEN … END` |
|--------|:-------:|:-----------------:|
| Standard SQL | No (Spark extension) | Yes |
| Number of branches | Exactly 2 | Unlimited |
| NULL-aware branching | Needs `IS NULL` | Needs `IS NULL` |
| Nesting | Gets unreadable quickly | Cleaner for 3+ branches |
| Conditional aggregation | Usable | Preferred |

---

## :material-flask-outline: Examples

### IF — simple two-branch

```sql
SELECT
    order_id,
    amount,
    IF(amount >= 100, 'high_value', 'standard') AS value_band
FROM orders;
```

### IIF (Databricks)

```sql
SELECT
    user_id,
    IIF(is_active, 'active', 'inactive') AS status
FROM users;
```

### Nested IF (use CASE WHEN instead for > 2 branches)

```sql
-- Acceptable for 2 levels, but prefer CASE for 3+
SELECT
    IF(score >= 90, 'A',
    IF(score >= 80, 'B',
    IF(score >= 70, 'C', 'F'))) AS grade
FROM results;
```

### IFNULL — replace NULL with default

```sql
SELECT
    customer_id,
    IFNULL(phone, 'N/A')    AS phone,
    IFNULL(loyalty_points, 0) AS loyalty_points
FROM customers;
```

### NULLIF — divide-by-zero guard

```sql
SELECT
    product_id,
    revenue,
    units_sold,
    ROUND(revenue / NULLIF(units_sold, 0), 2) AS revenue_per_unit
FROM sales;
-- NULLIF(units_sold, 0) returns NULL when units_sold = 0 → division yields NULL, not error
```

### COALESCE — first non-NULL from many sources

```sql
SELECT
    user_id,
    COALESCE(preferred_name, display_name, username, 'Anonymous') AS resolved_name
FROM user_profiles;
```

### COALESCE for multi-source joins

```sql
-- Take value from current row; fall back to previous batch if missing
SELECT
    a.user_id,
    COALESCE(a.email, b.email) AS email
FROM current_batch  AS a
FULL OUTER JOIN previous_batch AS b USING (user_id);
```

### NVL2 — branch on NULL vs non-NULL

```sql
-- If phone is not NULL → 'has phone', else → 'no phone'
SELECT user_id, NVL2(phone, 'has phone', 'no phone') AS phone_status
FROM users;
```

### Conditional default in aggregation

```sql
SELECT
    region,
    AVG(COALESCE(discount_pct, 0)) AS avg_discount
FROM orders
GROUP BY region;
```

### Safe percentage with NULLIF

```sql
SELECT
    region,
    shipped,
    total,
    ROUND(shipped * 100.0 / NULLIF(total, 0), 2) AS shipped_pct
FROM fulfillment_summary;
```

---

## :material-compare: COALESCE vs IFNULL vs NVL

All three replace NULL with a fallback — the differences are style and portability:

| Function | Args | Standard SQL | When to use |
|----------|:----:|:------------:|-------------|
| `COALESCE(a, b, …)` | 2+ | Yes | Multiple fallback sources |
| `IFNULL(a, b)` | 2 | No (MySQL/Spark) | Single fallback, concise |
| `NVL(a, b)` | 2 | No (Oracle/Spark) | Oracle-style codebase |

!!! tip
    Prefer `COALESCE` — it is standard SQL and works across all SQL dialects.

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| `IF(col = NULL, …)` | `col = NULL` is always NULL, not TRUE | `IF(col IS NULL, …)` |
| `NULLIF(col, NULL)` | Never matches — NULL != NULL | Use `IS NULL` check instead |
| Deep nested `IF` | Unreadable | Switch to `CASE WHEN` for 3+ branches |
| `IFNULL(col, 0)` in AVG | Changes the average (includes 0 for NULLs) | Use `COALESCE` only where 0 is semantically correct |

---

## :material-brain: When to Use

| Scenario | Function |
|----------|----------|
| Simple two-branch logic | `IF` / `IIF` |
| 3+ branches | `CASE WHEN` |
| Replace NULL with default | `COALESCE` / `IFNULL` |
| Divide-by-zero guard | `NULLIF(denominator, 0)` |
| NULL vs non-NULL branch | `NVL2` |
| First non-NULL across columns | `COALESCE(c1, c2, c3, …)` |
