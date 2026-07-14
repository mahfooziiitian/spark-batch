# :material-gate-and: Logical Operators

Logical operators combine boolean conditions to form compound predicates. Spark SQL
supports `AND`, `OR`, `NOT`, and three-valued logic where `NULL` propagates as
`UNKNOWN`.

---

## :material-code-tags: Syntax

```sql
-- AND: both conditions must be TRUE
WHERE condition_a AND condition_b

-- OR: at least one condition must be TRUE
WHERE condition_a OR condition_b

-- NOT: inverts the truth value
WHERE NOT condition

-- Compound
WHERE (condition_a OR condition_b) AND NOT condition_c
```

---

## :material-information-outline: Three-Valued Logic

Spark SQL uses **three-valued logic**: `TRUE`, `FALSE`, and `NULL` (UNKNOWN).

### AND truth table

| `A` | `B` | `A AND B` |
|-----|-----|-----------|
| TRUE | TRUE | TRUE |
| TRUE | FALSE | FALSE |
| TRUE | NULL | NULL |
| FALSE | FALSE | FALSE |
| FALSE | NULL | FALSE |
| NULL | NULL | NULL |

### OR truth table

| `A` | `B` | `A OR B` |
|-----|-----|----------|
| TRUE | TRUE | TRUE |
| TRUE | FALSE | TRUE |
| TRUE | NULL | TRUE |
| FALSE | FALSE | FALSE |
| FALSE | NULL | NULL |
| NULL | NULL | NULL |

### NOT truth table

| `A` | `NOT A` |
|-----|---------|
| TRUE | FALSE |
| FALSE | TRUE |
| NULL | NULL |

!!! note "NULL in WHERE"
    Rows where the `WHERE` predicate evaluates to `NULL` are **excluded** from the result —
    `NULL` is treated the same as `FALSE` for row filtering purposes.

---

## :material-flask-outline: Practical Examples

### AND — all conditions must hold

```sql
SELECT * FROM orders
WHERE status      = 'ACTIVE'
  AND region      = 'EU'
  AND amount      > 100
  AND order_date >= '2024-01-01';
```

### OR — any condition is sufficient

```sql
SELECT * FROM products
WHERE category = 'Electronics'
   OR category = 'Computers'
   OR brand    = 'Apple';
```

### OR with parentheses (prevent precedence errors)

```sql
-- GOOD: Correct: parentheses make the OR explicit
SELECT * FROM orders
WHERE status = 'PENDING'
  AND (region = 'EU' OR region = 'APAC');

-- BAD: Incorrect without parens: AND binds tighter than OR
-- Equivalent to: (status='PENDING' AND region='EU') OR region='APAC'
SELECT * FROM orders
WHERE status = 'PENDING'
  AND  region = 'EU'
   OR  region = 'APAC';
```

### NOT — negate a condition

```sql
SELECT * FROM users WHERE NOT is_deleted;

SELECT * FROM products WHERE NOT (price < 10 OR stock_qty = 0);
```

### NOT NULL behaviour

```sql
-- NULL discount is NOT equal to 0 — NOT applies to NULL → still NULL → excluded
SELECT * FROM orders WHERE NOT (discount = 0);
-- Rows where discount IS NULL are also excluded

-- GOOD: Include NULL discount rows explicitly
SELECT * FROM orders WHERE discount != 0 OR discount IS NULL;
```

### Short-circuit evaluation (optimization)

```sql
-- Spark evaluates left-to-right for AND; cheap filter first reduces rows early
SELECT *
FROM large_events
WHERE event_date = '2024-06-01'          -- partition filter: evaluated first
  AND event_type = 'purchase'            -- selective column filter
  AND complex_udf(payload) = 'match';   -- expensive UDF: last
```

### Combining AND / OR / NOT with IN and BETWEEN

```sql
SELECT *
FROM transactions
WHERE (amount BETWEEN 500 AND 5000 OR category = 'Premium')
  AND status NOT IN ('REFUNDED', 'CHARGEBACK')
  AND NOT (country = 'XX' AND is_flagged = TRUE);
```

### Logical operators in CASE WHEN

```sql
SELECT
    customer_id,
    CASE
        WHEN is_vip AND total_spent >= 10000         THEN 'Platinum'
        WHEN is_vip OR  total_spent >= 5000          THEN 'Gold'
        WHEN NOT is_vip AND total_spent < 500        THEN 'New'
        ELSE                                              'Standard'
    END AS tier
FROM customer_summary;
```

### De Morgan's laws — rewrite NOT (A AND B)

```sql
-- NOT (A AND B) ≡ NOT A OR NOT B
-- NOT (A OR B)  ≡ NOT A AND NOT B

-- BAD: Hard to read
SELECT * FROM orders WHERE NOT (status = 'CANCELLED' AND region = 'EU');

-- GOOD: Equivalent, clearer
SELECT * FROM orders WHERE status != 'CANCELLED' OR region != 'EU';
```

### NULL-aware OR with IS NULL

```sql
-- Include rows where discount is NULL or equals 0
SELECT * FROM orders WHERE discount = 0 OR discount IS NULL;

-- Equivalent using COALESCE
SELECT * FROM orders WHERE COALESCE(discount, 0) = 0;
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| All conditions required | `AND` |
| Any condition sufficient | `OR` |
| Invert a condition | `NOT condition` |
| Mixed AND/OR | Always add `()` around the OR group |
| NULL-inclusive OR | `col = val OR col IS NULL` |
| Rewrite complex NOT | Apply De Morgan's law for readability |
| Short-circuit expensive functions | Put cheap filters before expensive UDFs |
