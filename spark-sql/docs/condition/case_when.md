# :material-code-braces: CASE WHEN

`CASE WHEN` is SQL's primary conditional expression. It evaluates a list of conditions
in order and returns the value of the first branch that matches.
Unlike procedural `IF/ELSE`, it is an **expression** — it returns a single value per row
and can appear anywhere a column reference is valid.

---

## :material-pin: Syntax

### Searched CASE (most common)

```sql
CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ...
    ELSE default_result
END
```

### Simple CASE (value switch)

```sql
CASE expression
    WHEN value1 THEN result1
    WHEN value2 THEN result2
    ...
    ELSE default_result
END
```

!!! note
    The `ELSE` clause is optional. If omitted and no branch matches, the expression returns `NULL`.

---

## :material-table: Searched vs Simple CASE

| Feature | Searched CASE | Simple CASE |
|---------|:-------------:|:-----------:|
| Arbitrary conditions | :material-check: | :material-close: |
| Range checks | :material-check: | :material-close: |
| Equality-only checks | :material-check: | :material-check: |
| Readability for equality | Medium | High |
| NULL matching | Use `IS NULL` explicitly | Does NOT match NULL values |

---

## :material-flask-outline: Examples

### Tier classification (searched)

```sql
SELECT
    customer_id,
    total_spend,
    CASE
        WHEN total_spend >= 10000 THEN 'Platinum'
        WHEN total_spend >= 5000  THEN 'Gold'
        WHEN total_spend >= 1000  THEN 'Silver'
        ELSE 'Standard'
    END AS tier
FROM customers;
```

### Status label (simple)

```sql
SELECT
    order_id,
    status_code,
    CASE status_code
        WHEN 1 THEN 'Pending'
        WHEN 2 THEN 'Processing'
        WHEN 3 THEN 'Shipped'
        WHEN 4 THEN 'Delivered'
        ELSE 'Unknown'
    END AS status_label
FROM orders;
```

### NULL handling in CASE

```sql
SELECT
    user_id,
    email,
    CASE
        WHEN email IS NULL      THEN 'missing'
        WHEN email LIKE '%test%' THEN 'test account'
        ELSE 'valid'
    END AS email_status
FROM users;
```

### Conditional aggregation

```sql
SELECT
    region,
    COUNT(*)                                        AS total_orders,
    SUM(CASE WHEN status = 'shipped'  THEN 1 ELSE 0 END) AS shipped,
    SUM(CASE WHEN status = 'returned' THEN 1 ELSE 0 END) AS returned,
    ROUND(
        SUM(CASE WHEN status = 'returned' THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100, 2
    ) AS return_rate_pct
FROM orders
GROUP BY region;
```

!!! tip "FILTER is cleaner for conditional aggregation"
    ```sql
    COUNT(*) FILTER (WHERE status = 'returned') AS returned
    ```
    Produces the same result with less noise.

### CASE inside ORDER BY

```sql
-- Sort: active users first, then inactive, then NULL
SELECT * FROM users
ORDER BY
    CASE
        WHEN is_active IS NULL THEN 2
        WHEN is_active = TRUE  THEN 0
        ELSE 1
    END;
```

### Bucketing for GROUP BY

```sql
SELECT
    CASE
        WHEN age < 18  THEN 'under_18'
        WHEN age < 35  THEN '18_34'
        WHEN age < 55  THEN '35_54'
        ELSE '55_plus'
    END AS age_bucket,
    COUNT(*) AS users
FROM profiles
GROUP BY 1;
```

### Nested CASE (avoid deep nesting)

```sql
SELECT
    product_id,
    CASE
        WHEN category = 'electronics' THEN
            CASE
                WHEN price > 1000 THEN 'high-end electronics'
                ELSE 'budget electronics'
            END
        WHEN category = 'clothing' THEN 'apparel'
        ELSE 'other'
    END AS segment
FROM products;
```

!!! warning "Avoid deep nesting"
    Nested CASE is hard to read. Flatten with additional `WHEN` branches or a lookup join.

### Pivot with CASE

```sql
SELECT
    sale_date,
    SUM(CASE WHEN region = 'US' THEN revenue ELSE 0 END) AS us_revenue,
    SUM(CASE WHEN region = 'EU' THEN revenue ELSE 0 END) AS eu_revenue,
    SUM(CASE WHEN region = 'APAC' THEN revenue ELSE 0 END) AS apac_revenue
FROM daily_sales
GROUP BY sale_date
ORDER BY sale_date;
```

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| Omitting `ELSE` | Returns NULL for unmatched rows | Add `ELSE` with a safe default |
| Simple CASE for NULL | `CASE col WHEN NULL …` never matches | Use searched CASE with `WHEN col IS NULL` |
| Order matters | First matching branch wins — later branches are skipped | Put most specific conditions first |
| Using CASE in WHERE | Valid but verbose | Prefer direct predicates or CTEs |
| Deep nesting | Unreadable | Flatten into a single CASE or lookup table |

---

## :material-brain: When to Use

| Scenario | Pattern |
|----------|---------|
| Row classification / labelling | Searched CASE in SELECT |
| Value mapping (finite set) | Simple CASE |
| Conditional aggregation | CASE inside SUM / COUNT |
| Custom sort order | CASE inside ORDER BY |
| Pivot (dynamic columns) | CASE inside SUM + GROUP BY |
| NULL-aware branching | Searched CASE with `IS NULL` branch |
