# :material-set-none: Set Operators

Set operators combine or compare the result sets of two `SELECT` statements.
Spark SQL supports `UNION`, `UNION ALL`, `INTERSECT`, `INTERSECT ALL`, `EXCEPT`,
and `EXCEPT ALL`.

---

## :material-code-tags: Syntax

```sql
-- UNION: combine rows, remove duplicates
query_a UNION query_b;

-- UNION ALL: combine rows, keep duplicates (faster)
query_a UNION ALL query_b;

-- INTERSECT: rows in BOTH result sets (dedup)
query_a INTERSECT query_b;

-- INTERSECT ALL: rows in both, preserve duplicates
query_a INTERSECT ALL query_b;

-- EXCEPT (MINUS): rows in A but NOT in B (dedup)
query_a EXCEPT query_b;

-- EXCEPT ALL: rows in A minus those in B, preserve remaining duplicates
query_a EXCEPT ALL query_b;
```

Rules:
- Both queries must return the **same number of columns**.
- Column types must be **compatible** (Spark coerces compatible types automatically).
- Column **names** come from the first query.

---

## :material-information-outline: Behavior

1. `UNION` deduplicates — it internally applies a `DISTINCT` across all columns. This triggers a shuffle and is slower than `UNION ALL`.
2. `UNION ALL` appends rows without deduplication — always prefer it when duplicates are either impossible or acceptable.
3. `INTERSECT` / `EXCEPT` both deduplicate by default. Use the `ALL` variants to preserve duplicate counts.
4. `EXCEPT` is equivalent to a `LEFT ANTI JOIN` on all columns — rows in the left query that have no matching row in the right query.
5. `INTERSECT` / `EXCEPT` compare all selected columns, including `NULL` — two `NULL` values in the same column position are considered equal.
6. Set operators can be chained: `A UNION ALL B UNION ALL C`. Use parentheses to control evaluation order.

---

## :material-flask-outline: Practical Examples

### UNION ALL — combine two sources (no dedup)

```sql
-- Append current and historical orders into one result
SELECT order_id, customer_id, amount, order_date, 'current'    AS source
FROM orders_current

UNION ALL

SELECT order_id, customer_id, amount, order_date, 'historical' AS source
FROM orders_historical;
```

### UNION — merge and deduplicate

```sql
-- Unique customer IDs from two systems
SELECT customer_id FROM crm_customers
UNION
SELECT customer_id FROM ecommerce_customers;
```

### INTERSECT — customers in both systems

```sql
SELECT customer_id FROM crm_customers
INTERSECT
SELECT customer_id FROM ecommerce_customers;
```

### EXCEPT — customers in CRM but not eCommerce

```sql
SELECT customer_id FROM crm_customers
EXCEPT
SELECT customer_id FROM ecommerce_customers;
```

### EXCEPT for data reconciliation

```sql
-- Rows in staging that are NOT yet in the target table
SELECT order_id, customer_id, amount, order_date
FROM staging_orders

EXCEPT

SELECT order_id, customer_id, amount, order_date
FROM fact_orders;
-- These are the missing / new rows that need to be loaded
```

### INTERSECT ALL — find matching duplicate rows

```sql
-- Rows that exist in both tables including their duplicate counts
SELECT order_id, status FROM pending_orders
INTERSECT ALL
SELECT order_id, status FROM processed_orders;
```

### Chain three sources with UNION ALL

```sql
SELECT 'APAC'   AS region_group, order_id, amount FROM orders WHERE region = 'APAC'
UNION ALL
SELECT 'EMEA'   AS region_group, order_id, amount FROM orders WHERE region IN ('EU','ME','AF')
UNION ALL
SELECT 'AMER'   AS region_group, order_id, amount FROM orders WHERE region IN ('US','CA','LATAM')
ORDER BY region_group, amount DESC;
```

### UNION ALL to unpivot wide columns to rows

```sql
-- Convert monthly budget columns to rows (pre-Spark 3.4 UNPIVOT)
SELECT dept, 'Jan' AS month, jan_budget AS budget FROM monthly_budget
UNION ALL
SELECT dept, 'Feb',          feb_budget             FROM monthly_budget
UNION ALL
SELECT dept, 'Mar',          mar_budget             FROM monthly_budget
ORDER BY dept, month;
```

### EXCEPT as a quality check

```sql
-- Orders in the fact table whose order_id no longer exists in the source system
SELECT order_id FROM fact_orders
EXCEPT
SELECT order_id FROM source_orders;
-- These are phantom rows — investigate before deleting
```

### UNION ALL + GROUP BY — aggregate across combined sources

```sql
WITH all_events AS (
    SELECT user_id, event_type, event_date FROM mobile_events
    UNION ALL
    SELECT user_id, event_type, event_date FROM web_events
)
SELECT user_id, event_type, COUNT(*) AS event_count
FROM all_events
WHERE event_date >= '2024-01-01'
GROUP BY user_id, event_type;
```

---

## :material-swap-horizontal: Set Operator Comparison

| Operator | Deduplicates | Preserves order | Performance |
|----------|-------------|-----------------|-------------|
| `UNION ALL` | No | No | Fastest — no shuffle for dedup |
| `UNION` | Yes | No | Slower — dedup shuffle |
| `INTERSECT ALL` | No | No | Medium |
| `INTERSECT` | Yes | No | Medium |
| `EXCEPT ALL` | No | No | Medium |
| `EXCEPT` | Yes | No | Medium |

---

## :material-lightbulb-outline: When to Use

| Scenario | Operator |
|----------|---------|
| Append rows from multiple tables | `UNION ALL` (preferred) |
| Merge and deduplicate | `UNION` |
| Find common rows | `INTERSECT` |
| Find rows in A but not B | `EXCEPT` |
| Data reconciliation / gap detection | `EXCEPT` |
| Unpivot wide columns to rows | `UNION ALL` per column |

!!! tip "Always prefer UNION ALL over UNION"
    `UNION` triggers a full shuffle to deduplicate — it is equivalent to
    `UNION ALL` followed by `SELECT DISTINCT`. Use `UNION ALL` by default and
    add an explicit `SELECT DISTINCT` only when you actually need deduplication.
