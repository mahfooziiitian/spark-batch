# :material-file-tree: Subqueries

Subqueries are `SELECT` statements nested inside another query. In PySpark 4.2, Spark can execute them as scalar expressions, membership tests, existence checks, inline tables, or correlated lookups that Catalyst often rewrites into joins.

<script src="../../assets/js/querying-subquery-viz.js"></script>

### :material-animation-play: Interactive Visualization — Subquery Family Map

<div id="viz-subquery-index-map" class="ts-viz"></div>

Use the buttons to compare what each subquery form returns and how Spark 4.2 typically plans it.

______________________________________________________________________

## :material-sitemap: In This Section

| Page                                           | Covers                                         |
| ---------------------------------------------- | ---------------------------------------------- |
| [Scalar Subqueries](scalar.md)                 | One-row, one-column expression subqueries      |
| [IN / NOT IN](in-not-in.md)                    | Membership tests and the NULL trap             |
| [EXISTS / NOT EXISTS](exists.md)               | Semi-join and anti-join style existence checks |
| [Correlated Subqueries](correlated.md)         | Subqueries that reference outer-query columns  |
| [Derived Tables](derived-table.md)             | Subqueries in the `FROM` clause                |
| [Subquery in HAVING](having-subquery.md)       | Group filters driven by another query          |
| [Subquery vs JOIN vs CTE](subquery-vs-join.md) | Rewrites, equivalence, and fanout pitfalls     |

______________________________________________________________________

## :material-information-outline: Verified Spark 4.2 Behavior

| Topic                                  | Verified result                                                                |
| -------------------------------------- | ------------------------------------------------------------------------------ |
| Scalar subquery cardinality            | Exactly 1 column; more than 1 row raises `SCALAR_SUBQUERY_TOO_MANY_ROWS`       |
| Scalar subquery returning 0 rows       | Evaluates to `NULL`                                                            |
| `EXISTS` / `NOT EXISTS`                | Planned as `LeftSemi` / `LeftAnti` joins for common equality predicates        |
| `NOT IN` with a `NULL` on the right    | Returns zero rows because Spark uses null-aware anti semantics                 |
| Correlated equality subquery           | Often decorrelated into a hash join                                            |
| Correlated non-equality `EXISTS`       | Can fall back to `BroadcastNestedLoopJoin`                                     |
| Derived table alias after `FROM (...)` | Optional in Spark 4.2, though aliasing is still recommended                    |
| Inner expression aliases               | Needed for stable outer references like `total_spent` instead of `sum(amount)` |

______________________________________________________________________

## :material-code-tags: Fast Pattern Guide

| Need                                | Best pattern                         |
| ----------------------------------- | ------------------------------------ |
| Single threshold                    | Scalar subquery                      |
| Membership include                  | `IN` or `EXISTS`                     |
| Membership exclude on nullable data | `NOT EXISTS`                         |
| Inline aggregated table             | Derived table or CTE                 |
| Per-group comparison                | Correlated subquery or explicit join |
| Reusable intermediate result        | CTE                                  |

______________________________________________________________________

## :material-flask-outline: Minimal Verified Examples

```sql
-- Scalar: evaluated once because it is uncorrelated
SELECT order_id
FROM orders
WHERE amount > (SELECT AVG(amount) FROM orders);
```

```sql
-- NULL trap: returns zero rows if blocked_countries.country contains any NULL
SELECT region
FROM regions
WHERE region NOT IN (SELECT country FROM blocked_countries);
```

```sql
-- NULL-safe exclusion
SELECT region
FROM regions r
WHERE NOT EXISTS (
    SELECT 1
    FROM blocked_countries bc
    WHERE bc.country = r.region
);
```

```sql
-- Correlated equality subquery: Spark can rewrite this to a join
SELECT order_id, customer, amount
FROM orders o
WHERE amount > (
    SELECT AVG(amount)
    FROM orders i
    WHERE i.customer = o.customer
);
```

______________________________________________________________________

## :material-lightbulb-outline: Practical Rules

1. Prefer `NOT EXISTS` over `NOT IN` unless the subquery column is guaranteed non-null.
2. Alias inner aggregate expressions in derived tables so the outer query gets stable column names.
3. Check `EXPLAIN` when performance matters: a correlated equality subquery may become a hash join, while non-equality correlation can become a nested-loop join.
4. A join rewrite is not always equivalent unless you deduplicate the join side first.
