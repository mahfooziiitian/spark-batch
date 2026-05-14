# :material-scale-balance: Subquery vs JOIN vs CTE

Subqueries, JOINs, and CTEs often solve the same problem. Choosing the right tool
affects readability, debuggability, and — occasionally — performance.

---

## :material-swap-horizontal: Head-to-Head Comparison

| Aspect | Subquery | JOIN | CTE |
|--------|----------|------|-----|
| Readability | Good for simple checks | Verbose for existence checks | Best for multi-step logic |
| Reusability in same query | No | No (must repeat) | Yes — reference by name |
| NULL safety (`NOT`) | `NOT IN` unsafe; `NOT EXISTS` safe | `LEFT JOIN ... IS NULL` safe | Depends on what's inside |
| Duplicate handling | `IN`/`EXISTS` auto-deduplicate | Must add `DISTINCT` or dedup join side | Depends on CTE content |
| Optimizer flexibility | Limited (especially correlated) | Most options (broadcast, SMJ, hash) | Same as underlying query |
| Debuggability | Hard — hidden in outer query | Clear — two named tables | Best — name + isolate each step |
| Recursion | No | No | Yes (`WITH RECURSIVE`) |

---

## :material-flask-outline: Equivalent Rewrites

### Existence check

```sql
-- EXISTS (recommended for existence)
SELECT c.customer_id, c.name
FROM customers AS c
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- IN (equivalent, auto-deduplicates)
SELECT customer_id, name
FROM customers
WHERE customer_id IN (SELECT DISTINCT customer_id FROM orders);

-- INNER JOIN (equivalent — join side must be deduplicated)
SELECT DISTINCT c.customer_id, c.name
FROM customers AS c
JOIN orders AS o ON c.customer_id = o.customer_id;

-- CTE + INNER JOIN (most explicit)
WITH ordered AS (
    SELECT DISTINCT customer_id FROM orders
)
SELECT c.customer_id, c.name
FROM customers AS c
JOIN ordered AS o ON c.customer_id = o.customer_id;
```

### Anti-join (exclusion)

```sql
-- NOT EXISTS (recommended — NULL-safe)
SELECT c.customer_id, c.name
FROM customers AS c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id
);

-- NOT IN (dangerous if orders.customer_id is nullable)
SELECT customer_id, name
FROM customers
WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders);

-- LEFT JOIN + IS NULL (explicit, optimizer-friendly)
SELECT c.customer_id, c.name
FROM customers AS c
LEFT JOIN orders AS o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
```

### Per-group comparison

```sql
-- Correlated subquery (readable, may not decorrelate)
SELECT order_id, customer_id, amount
FROM orders AS o
WHERE amount > (
    SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id
);

-- JOIN with derived table (always efficient)
SELECT o.order_id, o.customer_id, o.amount
FROM orders AS o
JOIN (
    SELECT customer_id, AVG(amount) AS cust_avg
    FROM orders
    GROUP BY customer_id
) AS ca ON o.customer_id = ca.customer_id
WHERE o.amount > ca.cust_avg;

-- CTE version (most readable)
WITH customer_avgs AS (
    SELECT customer_id, AVG(amount) AS cust_avg
    FROM orders
    GROUP BY customer_id
)
SELECT o.order_id, o.customer_id, o.amount
FROM orders AS o
JOIN customer_avgs AS ca ON o.customer_id = ca.customer_id
WHERE o.amount > ca.cust_avg;
```

### Post-aggregation filter

```sql
-- Derived table (subquery in FROM)
SELECT region, total_revenue
FROM (
    SELECT region, SUM(amount) AS total_revenue
    FROM orders GROUP BY region
) AS rt
WHERE total_revenue > 500000;

-- HAVING (most concise for simple threshold)
SELECT region, SUM(amount) AS total_revenue
FROM orders
GROUP BY region
HAVING SUM(amount) > 500000;

-- CTE (best when threshold is complex or reused)
WITH region_totals AS (
    SELECT region, SUM(amount) AS total_revenue
    FROM orders
    GROUP BY region
)
SELECT region, total_revenue
FROM region_totals
WHERE total_revenue > 500000;
```

---

## :material-lightbulb-outline: Decision Guide

```
Simple existence check (has at least one related row)?
  → EXISTS

Exclusion (no matching row), column might be NULL?
  → NOT EXISTS  (never NOT IN on nullable columns)

Exclusion, column guaranteed NOT NULL?
  → NOT IN or LEFT JOIN + IS NULL

Filter against a single computed threshold?
  → Scalar subquery in WHERE

Per-row comparison against group aggregate?
  Small table  → correlated subquery (Catalyst will decorrelate)
  Large table  → explicit JOIN with aggregated derived table / CTE

Multi-step pipeline / result reused more than once?
  → CTE

Recursive / hierarchical data?
  → WITH RECURSIVE CTE

Need broadcast hint or fine-grained join strategy control?
  → Explicit JOIN (add /*+ BROADCAST(...) */ hint)
```

---

## :material-shield-outline: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| `NOT IN` on nullable column | Returns zero rows | Use `NOT EXISTS` |
| Correlated scalar returns > 1 row | Runtime exception | Add `LIMIT 1` or use `MAX`/`MIN` |
| `IN` on large subquery | Full subquery evaluated, may be slow | Rewrite as `JOIN` |
| Deep nested subqueries | Unreadable, hard to debug | Refactor into CTEs |
| Missing alias on derived table | `AnalysisException` | Always alias the `(SELECT ...) AS name` |
| Joining without deduplication | Fanout / row multiplication | Add `DISTINCT` or deduplicate the join side |

!!! tip "When in doubt, use a CTE"
    CTEs can always replace subqueries and derived tables. They are easier to test
    (run the CTE alone), easier to read (named, top-down), and produce identical plans.
    Start with a CTE and simplify to a subquery only when the query is a genuine one-liner.
