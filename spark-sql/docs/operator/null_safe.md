# :material-null: Null-Safe Operators

Standard SQL comparisons return `NULL` (UNKNOWN) whenever either operand is `NULL`.
Null-safe operators give you explicit control over `NULL` equality and inequality,
which is critical for joins, filters, and deduplication on nullable columns.

---

## :material-code-tags: Syntax

| Operator / Expression | Description | Returns when both NULL |
|-----------------------|-------------|----------------------|
| `col IS NULL` | True if col is NULL | `TRUE` |
| `col IS NOT NULL` | True if col is not NULL | `FALSE` |
| `a <=> b` | Null-safe equality | `TRUE` |
| `NOT (a <=> b)` | Null-safe inequality | `FALSE` |
| `a IS DISTINCT FROM b` | NULL-safe not-equal | `FALSE` |
| `a IS NOT DISTINCT FROM b` | NULL-safe equal (alias for `<=>`) | `TRUE` |
| `COALESCE(a, b, ...)` | First non-NULL value | Returns next non-null fallback |
| `NULLIF(a, b)` | NULL if a = b, else a | Returns `a` |
| `NVL(a, b)` | Alias for `COALESCE(a, b)` | Returns `b` |

---

## :material-information-outline: Behavior

1. `IS NULL` / `IS NOT NULL` are the only standard ways to test for `NULL` — `col = NULL` always returns `NULL`, never `TRUE`.
2. `<=>` (null-safe equal) returns `TRUE` when both sides are `NULL`, and `FALSE` when exactly one side is `NULL`. Useful in `JOIN ON` and `MERGE ON` conditions.
3. `IS DISTINCT FROM` returns `TRUE` when values differ **or** when one is `NULL` and the other is not — the most readable null-safe inequality.
4. `IS NOT DISTINCT FROM` is an alias for `<=>` and is preferred in ANSI SQL contexts for readability.
5. `COALESCE` evaluates arguments left-to-right and returns the first non-`NULL` value.
6. `NULLIF(a, b)` returns `NULL` when `a = b` — commonly used to avoid division-by-zero: `numerator / NULLIF(denominator, 0)`.

---

## :material-flask-outline: Practical Examples

### IS NULL / IS NOT NULL

```sql
-- Find rows with no email address
SELECT customer_id, name FROM customers WHERE email IS NULL;

-- Filter out rows with any NULL in critical columns
SELECT * FROM orders
WHERE customer_id IS NOT NULL
  AND amount      IS NOT NULL
  AND order_date  IS NOT NULL;
```

### NULL-safe equality in JOIN (<=>)

```sql
-- Standard join drops rows when either FK is NULL
SELECT o.order_id, c.name
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.customer_id;       -- drops NULLs

-- Null-safe join keeps rows where both are NULL
SELECT o.order_id, c.name
FROM orders AS o
JOIN customers AS c ON o.customer_id <=> c.customer_id;     -- NULL <=> NULL = TRUE
```

### IS DISTINCT FROM — null-safe inequality

```sql
-- Rows where old_region and new_region differ (including NULL vs non-NULL)
SELECT customer_id, old_region, new_region
FROM customer_changes
WHERE old_region IS DISTINCT FROM new_region;

-- Equivalent without the operator:
WHERE NOT (old_region <=> new_region)
```

### IS NOT DISTINCT FROM — null-safe equality (readable)

```sql
-- Same as <=> but reads more like English
SELECT * FROM audit_log
WHERE old_value IS NOT DISTINCT FROM new_value;  -- no change detected
```

### COALESCE — substitute default for NULL

```sql
SELECT
    order_id,
    COALESCE(discount, 0)                         AS discount,
    COALESCE(shipping_name, billing_name, name)   AS recipient,
    COALESCE(notes, 'No notes')                   AS notes
FROM orders;
```

### NULLIF — null-on-match (division-by-zero guard)

```sql
SELECT
    product_id,
    revenue,
    units_sold,
    ROUND(revenue / NULLIF(units_sold, 0), 4)  AS revenue_per_unit,
    NULLIF(TRIM(description), '')              AS description   -- empty string → NULL
FROM products;
```

### NULL in CASE WHEN

```sql
SELECT
    customer_id,
    CASE
        WHEN region IS NULL THEN 'Unknown'
        WHEN region = 'EU'  THEN 'Europe'
        ELSE region
    END AS region_label
FROM customers;
```

### NULL-safe change detection for SCD

```sql
-- Use <=> to detect changed columns even when old or new value is NULL
SELECT
    customer_id,
    md5(concat_ws('||',
        COALESCE(name,  ''),
        COALESCE(email, ''),
        COALESCE(city,  '')
    )) AS row_hash
FROM staging_customers;

-- In MERGE: flag as changed when hash differs (NULL-safe because COALESCE handles NULLs)
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET ...
```

### NOT IN NULL trap and fix

```sql
-- BAD: Returns 0 rows because country column contains NULLs
SELECT customer_id FROM customers
WHERE country NOT IN (SELECT country FROM blocked_countries);

-- GOOD: Fix 1: exclude NULLs from the subquery
SELECT customer_id FROM customers
WHERE country NOT IN (
    SELECT country FROM blocked_countries WHERE country IS NOT NULL
);

-- GOOD: Fix 2: use IS DISTINCT FROM logic via NOT EXISTS
SELECT c.customer_id FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1 FROM blocked_countries bc WHERE c.country IS NOT DISTINCT FROM bc.country
);
```

---

## :material-swap-horizontal: Operator Comparison

| Goal | Expression | NULL-safe? |
|------|-----------|-----------|
| Check for NULL | `col IS NULL` | Yes |
| Check for non-NULL | `col IS NOT NULL` | Yes |
| Equal (drops NULLs) | `a = b` | No |
| Equal (keeps NULLs) | `a <=> b` | Yes |
| Not equal (drops NULLs) | `a != b` | No |
| Not equal (keeps NULLs) | `a IS DISTINCT FROM b` | Yes |
| First non-NULL value | `COALESCE(a, b, ...)` | Yes |
| NULL on equality | `NULLIF(a, b)` | Partial |

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Test a column for NULL | `IS NULL` / `IS NOT NULL` |
| JOIN on nullable foreign key | `ON a <=> b` |
| Detect changed columns with potential NULLs | `IS DISTINCT FROM` |
| Substitute a default for NULL | `COALESCE(col, default)` |
| Avoid division by zero | `NULLIF(denominator, 0)` |
| Exclude NULLs from NOT IN subquery | Add `WHERE col IS NOT NULL` to subquery |

!!! warning "Never use = NULL or != NULL"
    `col = NULL` and `col != NULL` always evaluate to `NULL` — they never return
    `TRUE`. Always use `IS NULL` and `IS NOT NULL` to test for null values.
