# :material-scale-balance: Subquery vs JOIN vs CTE

Subqueries, joins, and CTEs often express the same business rule, but they are not always equivalent. In Spark 4.2 the optimizer rewrites many subqueries into joins, while plain joins can still change row counts if the right side is not deduplicated.

<script src="../../assets/js/querying-subquery-viz.js"></script>

### :material-animation-play: Interactive Visualization — Same Logic, Different Row Counts

<div id="viz-subquery-join-fanout" class="ts-viz"></div>

Use the duplicate toggle to see why `IN` or `EXISTS` can match a business rule while a plain join silently multiplies rows.

______________________________________________________________________

## :material-swap-horizontal: Head-to-Head

| Aspect              | Subquery                                  | JOIN                            | CTE                           |
| ------------------- | ----------------------------------------- | ------------------------------- | ----------------------------- |
| Best for            | Scalar thresholds, existence, inline sets | Explicit relational combination | Naming multi-step logic       |
| Reusability         | Local only                                | Local only                      | Reusable within the statement |
| NULL-safe exclusion | `NOT EXISTS`                              | `LEFT JOIN ... IS NULL`         | Depends on inner logic        |
| Duplicate handling  | `IN` / `EXISTS` do not fan out            | Fanout possible                 | Depends on downstream joins   |
| Optimizer freedom   | Often rewritten to joins                  | Full join planning              | Same as underlying query      |

______________________________________________________________________

## :material-information-outline: Verified Spark 4.2 Findings

1. `IN` and `EXISTS` on equality predicates optimized to `LeftSemi` joins during verification.
2. A correlated aggregate filter optimized to a hash join, so a subquery is not automatically slower than a join rewrite.
3. An uncorrelated scalar subquery stayed as a `Subquery` node instead of becoming a join.
4. A plain inner join is **not** equivalent to `IN` or `EXISTS` when the join side contains duplicates.
5. `NOT EXISTS` and `LEFT JOIN ... IS NULL` are often equivalent; `NOT IN` is not equivalent once `NULL` enters the right side.

______________________________________________________________________

## :material-flask-outline: Verified Equivalence and Non-Equivalence

### Equivalent after deduplication

```sql
SELECT customer_id, name
FROM customers
WHERE customer_id IN (SELECT customer_id FROM customer_orders);
```

```sql
SELECT DISTINCT c.customer_id, c.name
FROM customers c
JOIN customer_orders o
    ON c.customer_id = o.customer_id;
```

Both returned customer ids `1` and `2` in PySpark 4.2.

### Not equivalent without deduplication

```sql
SELECT c.customer_id, c.name
FROM customers c
JOIN customer_orders o
    ON c.customer_id = o.customer_id
ORDER BY c.customer_id, o.order_id;
```

Because customer ids `1` and `2` each had multiple orders, the join returned duplicate customer rows.

### Safe anti-join rewrite

```sql
SELECT c.customer_id, c.name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM customer_orders o
    WHERE o.customer_id = c.customer_id
);
```

Equivalent explicit join form:

```sql
SELECT c.customer_id, c.name
FROM customers c
LEFT JOIN customer_orders o
    ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
```

______________________________________________________________________

## :material-lightbulb-outline: Decision Guide

| Need                                    | Best starting point                       |
| --------------------------------------- | ----------------------------------------- |
| One scalar value                        | Scalar subquery                           |
| Has any related row                     | `EXISTS`                                  |
| Has no related row, nullable right side | `NOT EXISTS`                              |
| Heavy relational reshaping              | Explicit `JOIN`                           |
| Multi-step readable pipeline            | CTE                                       |
| Join rewrite would duplicate rows       | Deduplicate first or keep `IN` / `EXISTS` |
