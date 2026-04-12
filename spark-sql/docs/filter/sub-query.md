# :material-file-tree: Subquery Filters

Subqueries let you filter rows based on the results of another query.
Spark SQL supports `IN`, `EXISTS`, and scalar subqueries.

---

## 📌 Syntax Patterns

### IN

```sql
SELECT * FROM orders
WHERE customer_id IN (SELECT id FROM customers WHERE country = 'US');
```

### EXISTS

```sql
SELECT * FROM products p
WHERE EXISTS (
  SELECT 1
  FROM inventory i
  WHERE i.product_id = p.id
    AND i.stock > 0
);
```

### NOT EXISTS (Preferred over NOT IN)

```sql
SELECT * FROM customers c
WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.customer_id = c.id
);
```

---

## 🔍 NOT IN and NULLs

`NOT IN` returns no rows if the subquery returns **any NULLs**.
Prefer `NOT EXISTS` to avoid this pitfall.

```sql
-- Dangerous when subquery can return NULLs
SELECT * FROM customers
WHERE customer_id NOT IN (SELECT customer_id FROM orders);
```

---

## 🧪 Practical Examples

### Correlated Subquery

```sql
SELECT *
FROM orders o
WHERE amount > (
  SELECT AVG(amount)
  FROM orders
  WHERE customer_id = o.customer_id
);
```

### Filter by Latest Record

```sql
SELECT *
FROM order_events e
WHERE event_time = (
  SELECT MAX(event_time)
  FROM order_events
  WHERE order_id = e.order_id
);
```

---

## 🧠 When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Filter using a lookup list | `IN` |
| Filter with a correlated condition | `EXISTS` |
| Exclude rows based on another table | `NOT EXISTS` |
| Single-value comparison | Scalar subquery |

---

> **Tip:** Large `IN` lists are often faster as a join or broadcast join.
