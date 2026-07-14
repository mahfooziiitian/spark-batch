# :material-logic-and: Logical Conditions

Logical operators combine or invert predicates to build complex filter expressions.
Spark SQL uses **three-valued logic** — every operand can be TRUE, FALSE, or NULL (UNKNOWN).

---

## :material-pin: Operator Reference

| Operator | Syntax | Description |
|----------|--------|-------------|
| `AND` | `A AND B` | TRUE only when both operands are TRUE |
| `OR` | `A OR B` | TRUE when at least one operand is TRUE |
| `NOT` | `NOT A` | Inverts TRUE ↔ FALSE; NOT NULL = NULL |
| `EXISTS` | `EXISTS (subquery)` | TRUE if subquery returns any row |
| `NOT EXISTS` | `NOT EXISTS (subquery)` | TRUE if subquery returns no rows |

---

## :material-table: Truth Tables

### AND

| A | B | A AND B |
|---|---|---------|
| TRUE | TRUE | **TRUE** |
| TRUE | FALSE | FALSE |
| TRUE | NULL | NULL |
| FALSE | FALSE | FALSE |
| FALSE | NULL | **FALSE** |
| NULL | NULL | NULL |

### OR

| A | B | A OR B |
|---|---|--------|
| TRUE | TRUE | **TRUE** |
| TRUE | FALSE | **TRUE** |
| TRUE | NULL | **TRUE** |
| FALSE | FALSE | FALSE |
| FALSE | NULL | NULL |
| NULL | NULL | NULL |

### NOT

| A | NOT A |
|---|-------|
| TRUE | FALSE |
| FALSE | TRUE |
| NULL | NULL |

!!! note "Key insight"
    `FALSE AND NULL = FALSE` (short-circuit — FALSE dominates AND).
    `TRUE OR NULL = TRUE` (short-circuit — TRUE dominates OR).

---

## :material-sort-ascending: Operator Precedence

Highest to lowest:

1. `NOT`
2. `AND`
3. `OR`

```sql
-- Parsed as: A AND (B OR C) — parentheses make intent clear
WHERE A AND (B OR C)

-- Without parentheses: (A AND B) OR C — different semantics!
WHERE A AND B OR C
```

!!! warning
    Always use explicit parentheses when mixing `AND` and `OR`. Relying on precedence
    leads to subtle bugs that are hard to spot in code review.

---

## :material-flask-outline: Examples

### Combine requirements with AND

```sql
SELECT * FROM orders
WHERE status = 'shipped'
  AND amount > 100
  AND order_date >= '2024-01-01';
```

### Allow alternatives with OR

```sql
SELECT * FROM users
WHERE tier = 'gold' OR tier = 'platinum';

-- Prefer IN for readability with many values
SELECT * FROM users
WHERE tier IN ('gold', 'platinum', 'diamond');
```

### Parentheses for correct grouping

```sql
-- Users in US or CA who are active AND have purchased recently
SELECT * FROM users
WHERE (country = 'US' OR country = 'CA')
  AND is_active = TRUE
  AND last_purchase_date >= '2024-01-01';
```

### NOT to exclude conditions

```sql
SELECT * FROM events
WHERE NOT (event_type = 'test' OR event_type = 'debug');

-- Equivalent (often clearer)
SELECT * FROM events
WHERE event_type NOT IN ('test', 'debug');
```

### EXISTS — semi-join pattern

```sql
-- Orders from customers who have placed more than 5 orders total
SELECT o.*
FROM orders o
WHERE EXISTS (
    SELECT 1
    FROM orders sub
    WHERE sub.customer_id = o.customer_id
    GROUP BY sub.customer_id
    HAVING COUNT(*) > 5
);
```

### NOT EXISTS — anti-join pattern

```sql
-- Customers with no orders in the last 90 days
SELECT c.*
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id
      AND o.order_date >= current_date() - INTERVAL 90 DAYS
);
```

### NULL propagation in logic

```sql
-- NULL in AND chain: if any condition is NULL, result may be NULL
SELECT *
FROM events
WHERE (device_id IS NOT NULL)        -- guard first
  AND (device_id = '12345');
```

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| `NOT IN (subquery)` with NULLs | Returns no rows | Use `NOT EXISTS` |
| `OR` without parentheses | Wrong operator precedence | Add `(…)` around OR groups |
| `NOT NULL` | Not valid — should be `IS NOT NULL` | `col IS NOT NULL` |
| `WHERE flag = NULL` | NULL = NULL is NULL, not TRUE | `WHERE flag IS NULL` |
| `OR` on indexed column | Prevents index use (full scan) | Rewrite as `UNION ALL` |

---

## :material-brain: When to Use

| Scenario | Pattern |
|----------|---------|
| All conditions must hold | `AND` |
| Any condition is enough | `OR` |
| Exclude rows matching condition | `NOT (…)` or `NOT IN` (safe) |
| Anti-join (exclude matched rows) | `NOT EXISTS` |
| Semi-join (filter by related rows) | `EXISTS` |
| Mixed AND/OR | Always use explicit `(…)` |


Logical operators combine or invert predicates to build more complex filters.

---

## :material-pin: Operators

| Operator | Example | Description |
|----------|---------|-------------|
| `AND` | `A AND B` | True when both are true |
| `OR` | `A OR B` | True when either is true |
| `NOT` | `NOT A` | Inverts a condition |

---

## :material-magnify: Precedence

`NOT` is evaluated first, then `AND`, then `OR`.
Use parentheses to make precedence explicit.

---

## :material-flask-outline: Practical Examples

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

## :material-brain: When to Use

| Scenario | Pattern |
|----------|---------|
| Combine requirements | `AND` |
| Allow alternatives | `OR` |
| Exclude matches | `NOT` |
| Avoid ambiguity | Parentheses |

---

> **Tip:** Prefer explicit parentheses when mixing `AND` and `OR` to avoid
> logical mistakes.
