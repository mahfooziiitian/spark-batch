# :material-swap-horizontal: CTE vs Temp View vs Cache

Three mechanisms can name and reuse an intermediate result in Spark SQL. Choosing the
right one avoids unnecessary re-computation, excessive memory use, and session-scoping
bugs.

---

## :material-swap-horizontal: Comparison

| Aspect | CTE (`WITH`) | Temp View (`CREATE TEMP VIEW`) | Cached Table (`CACHE TABLE`) |
|--------|-------------|-------------------------------|------------------------------|
| Scope | Single statement | Entire session (until dropped or session ends) | Entire session |
| Materialized | No — inlined at each reference site | No — re-evaluated on each query | Yes — stored in Spark memory |
| Re-evaluation on multiple references | Yes (N references = N evaluations) | Yes | No (read from memory) |
| Syntax overhead | Low — defined inline | Medium — separate DDL statement | Medium — separate `CACHE` statement |
| Cross-statement reuse | No | Yes | Yes |
| Ideal for | One-off complex queries, pipelines | Shared intermediate across multiple queries | Expensive intermediate used repeatedly |

---

## :material-information-outline: When Each Option Wins

### Use a CTE when:
- The logic is only needed within one statement.
- The query is self-contained and must be portable (no session state required).
- The CTE is small or cheap, or is only referenced once.

### Use a Temp View when:
- Multiple downstream queries reference the same intermediate result.
- The intermediate result must be shared with a different `WITH` block or a separate `INSERT` statement.
- You want to inspect the intermediate result interactively.

### Use CACHE TABLE when:
- The same expensive intermediate is referenced in several queries in the same session/notebook.
- The dataset fits comfortably in executor memory.
- The source data does not change during the session.

---

## :material-flask-outline: Practical Examples

### CTE — single-statement, no reuse

```sql
-- Self-contained: CTE is only used here
WITH active_customers AS (
    SELECT customer_id, name, region
    FROM customers
    WHERE status = 'ACTIVE'
)
SELECT region, COUNT(*) AS active_count
FROM active_customers
GROUP BY region;
```

### Temp View — shared across multiple statements

```sql
-- Create once, reuse in several queries
CREATE OR REPLACE TEMP VIEW active_customers AS
SELECT customer_id, name, region
FROM customers
WHERE status = 'ACTIVE';

-- Query 1: regional count
SELECT region, COUNT(*) AS active_count
FROM active_customers
GROUP BY region;

-- Query 2: join with orders in a separate statement
SELECT
    ac.region,
    SUM(o.amount) AS regional_revenue
FROM active_customers AS ac
JOIN orders AS o ON ac.customer_id = o.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY ac.region;
```

### CACHE TABLE — expensive intermediate used many times

```sql
-- Expensive: large aggregation over months of data
CREATE OR REPLACE TEMP VIEW customer_ltv AS
SELECT
    customer_id,
    SUM(amount)  AS lifetime_value,
    COUNT(*)     AS order_count,
    MAX(order_date) AS last_order
FROM orders
GROUP BY customer_id;

-- Cache it once in memory
CACHE TABLE customer_ltv;

-- Now used in multiple downstream queries — reads from memory each time
SELECT * FROM customer_ltv WHERE lifetime_value > 1000;
SELECT * FROM customer_ltv ORDER BY order_count DESC LIMIT 20;
SELECT region, AVG(lifetime_value) FROM customer_ltv JOIN customers USING (customer_id) GROUP BY region;

-- Release memory when done
UNCACHE TABLE customer_ltv;
```

### CTE vs Temp View: same result set, different scope

```sql
-- ✅ CTE: works perfectly for a single INSERT pipeline
WITH prepared AS (
    SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
)
INSERT INTO customer_summary SELECT * FROM prepared;

-- ✅ Temp View: needed when a second INSERT also uses the same intermediate
CREATE OR REPLACE TEMP VIEW prepared AS
SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id;

INSERT INTO customer_summary SELECT * FROM prepared;
INSERT INTO customer_archive  SELECT * FROM prepared WHERE total < 50;
```

### Checking if a temp view is materialized (EXPLAIN)

```sql
-- A temp view is NOT cached — each reference re-runs the query
EXPLAIN SELECT * FROM active_customers;
-- Output includes FileScan — confirms re-evaluation

-- After CACHE TABLE, the plan changes
CACHE TABLE active_customers;
EXPLAIN SELECT * FROM active_customers;
-- Output includes InMemoryTableScan — reads from cache
```

---

## :material-lightbulb-outline: Decision Guide

```
Is the intermediate only used within a single SQL statement?
  Yes → CTE (WITH clause)

Is it used across multiple statements in the same session?
  Yes, cheap to recompute → Temp View
  Yes, expensive to recompute → Temp View + CACHE TABLE

Does source data change between queries?
  Yes → Do NOT cache (stale reads) → use CTE or Temp View without caching

Is the dataset too large to fit in executor memory?
  Yes → Do NOT cache → use Temp View or CTE
```

!!! warning "Stale cache"
    `CACHE TABLE` stores a snapshot of the data at cache time. If the underlying table
    is updated after caching, the cached view becomes stale. Always `UNCACHE TABLE`
    and re-cache after writing new data.

!!! tip "LAZY caching"
    `CACHE LAZY TABLE view_name` defers materialization until the first query hits the
    view, avoiding upfront computation cost if the view is never actually queried.
