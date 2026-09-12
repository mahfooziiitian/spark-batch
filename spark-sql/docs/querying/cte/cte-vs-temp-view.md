# :material-swap-horizontal: CTE vs Temp View vs Cache

CTEs, temp views, and caching all give a name to intermediate data, but they solve different problems. In Spark 4.2, the biggest practical differences are statement scope, cross-statement reuse, and whether the plan still expands back to the source query.

### :material-animation-play: Interactive Visualization — Scope and Plan Reuse

<div id="viz-cte-vs-view" class="ts-viz"></div>

The comparison shows the verified progression from inline logic to session-scoped reuse to in-memory reuse. In Spark 4.2, switching from CTE to temp view alone does not force materialization; caching is the step that changes the plan shape.

______________________________________________________________________

## :material-swap-horizontal: Comparison

| Aspect                    | CTE (`WITH`)     | Temp View (`CREATE TEMP VIEW`)        | Cached Temp View (`CACHE TABLE`)    |
| ------------------------- | ---------------- | ------------------------------------- | ----------------------------------- |
| Scope                     | Single statement | Entire Spark session                  | Entire Spark session                |
| Reused across statements  | No               | Yes                                   | Yes                                 |
| Materialized by default   | No               | No                                    | Yes, after cache population         |
| Typical plan before cache | Expanded inline  | Expanded inline                       | `InMemoryRelation` / in-memory scan |
| Good default for          | One-off logic    | Shared logic in a notebook or session | Expensive reused logic              |

______________________________________________________________________

## :material-information-outline: Verified Behavior in Spark 4.2

1. **CTE scope ends with the statement** — the name disappears immediately after that query finishes.
2. **Temp views persist for the session** — a second statement can query the same temp view without redefining it.
3. **Temp views are not automatically materialized** — `EXPLAIN` for an uncached temp view still showed the underlying `Range` plan in the verified example.
4. **A temp view does not imply compute-once reuse** — an identical self-join built from a CTE and from an uncached temp view produced the same duplicated aggregate branches in Spark 4.2.
5. **Caching changes the plan** — after `CACHE TABLE active_customers` and one query to populate it, `EXPLAIN` showed `InMemoryRelation` instead of only the source plan.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### CTE: one statement, no session state

```sql
WITH active_customers AS (
    SELECT customer_id, name, region
    FROM customers
    WHERE status = 'ACTIVE'
)
SELECT region, COUNT(*) AS active_count
FROM active_customers
GROUP BY region;
```

### Temp view: reuse across statements

```sql
CREATE OR REPLACE TEMP VIEW active_customers AS
SELECT customer_id, name, region
FROM customers
WHERE status = 'ACTIVE';

SELECT region, COUNT(*) AS active_count
FROM active_customers
GROUP BY region;

SELECT
    ac.region,
    SUM(o.amount) AS regional_revenue
FROM active_customers AS ac
JOIN orders AS o ON ac.customer_id = o.customer_id
WHERE o.order_date >= DATE '2024-01-01'
GROUP BY ac.region;
```

### Cache: keep a reused view in memory

```sql
CREATE OR REPLACE TEMP VIEW customer_ltv AS
SELECT
    customer_id,
    SUM(amount) AS lifetime_value,
    COUNT(*) AS order_count,
    MAX(order_date) AS last_order
FROM orders
GROUP BY customer_id;

CACHE TABLE customer_ltv;

SELECT * FROM customer_ltv WHERE lifetime_value > 1000;
SELECT * FROM customer_ltv ORDER BY order_count DESC LIMIT 20;
SELECT region, AVG(lifetime_value)
FROM customer_ltv
JOIN customers USING (customer_id)
GROUP BY region;

UNCACHE TABLE customer_ltv;
```

### `EXPLAIN` before and after cache

```sql
CREATE OR REPLACE TEMP VIEW active_customers AS
SELECT id, CASE WHEN id % 2 = 0 THEN 'ACTIVE' ELSE 'INACTIVE' END AS status
FROM range(0, 10);

EXPLAIN SELECT * FROM active_customers WHERE status = 'ACTIVE';
-- Verified in Spark 4.2: the plan still expands to Filter + Range.

CACHE TABLE active_customers;
SELECT COUNT(*) FROM active_customers;
EXPLAIN SELECT * FROM active_customers WHERE status = 'ACTIVE';
-- Verified in Spark 4.2: the plan includes InMemoryRelation.
```

______________________________________________________________________

## :material-lightbulb-outline: Decision Guide

| Need                                                       | Best Fit          |
| ---------------------------------------------------------- | ----------------- |
| Readable logic inside one statement                        | CTE               |
| Reuse across several queries in the same session           | Temp view         |
| Reuse across several queries without recomputing each time | Temp view + cache |
| A reusable debugging surface you can inspect separately    | Temp view         |
| The lightest possible syntax                               | CTE               |

!!! tip "Promote only when needed"

    Start with a CTE. Move it to a temp view only when multiple statements need it,
    and cache it only when repeated recomputation is measurably expensive.
