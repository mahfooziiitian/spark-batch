# :material-table-arrow-right: Derived Tables (Inline Views)

A derived table is a subquery inside the `FROM` clause. In Spark 4.2 it behaves like an inline relation that the optimizer can still prune and push predicates through.

<script src="../../assets/js/querying-subquery-viz.js"></script>

### :material-animation-play: Interactive Visualization — Naming a Derived Table

<div id="viz-subquery-derived-aliasing" class="ts-viz"></div>

This comparison shows what Spark 4.2 actually requires: the relation alias after `FROM (...)` is optional, but inner expression aliases are what make outer references readable.

______________________________________________________________________

## :material-code-tags: Basic Form

```sql
SELECT customer, total_spent
FROM (
    SELECT customer, SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer
) AS customer_totals
WHERE total_spent > 200;
```

______________________________________________________________________

## :material-information-outline: Verified Spark 4.2 Behavior

1. A derived table **does not need** a relation alias in Spark 4.2. `FROM (SELECT ...)` ran successfully during verification.
2. A relation alias is still recommended when you want to qualify columns, self-join, or make the query readable.
3. Inner expression aliases matter. Without `AS total_spent`, Spark exposes the generated column name `sum(amount)`.
4. Outer filters can be pushed into or around the derived table when the optimizer proves it is safe.
5. For one-off inline logic, a derived table and a CTE usually optimize to the same plan shape.

______________________________________________________________________

## :material-flask-outline: Verified Examples

### Works without a relation alias

```sql
SELECT *
FROM (
    SELECT customer, SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer
)
WHERE total_spent > 200;
```

Spark 4.2 returned `Bob` and `Alice`; it did **not** raise an alias error.

### Stable outer reference with an inner alias

```sql
SELECT customer, total_spent
FROM (
    SELECT customer, SUM(amount) AS total_spent
    FROM orders
    GROUP BY customer
) AS customer_totals
ORDER BY total_spent DESC;
```

### What happens without the inner alias

```sql
SELECT total_spent
FROM (
    SELECT customer, SUM(amount)
    FROM orders
    GROUP BY customer
) AS customer_totals;
```

Spark 4.2 raised `UNRESOLVED_COLUMN.WITH_SUGGESTION` because the output column name was actually `sum(amount)`.

______________________________________________________________________

## :material-swap-horizontal: Derived Table vs CTE

| Aspect       | Derived table            | CTE                                 |
| ------------ | ------------------------ | ----------------------------------- |
| Scope        | Only where written       | Named for the rest of the statement |
| Best for     | Small inline step        | Multi-step or reused logic          |
| Optimization | Same as underlying query | Same as underlying query            |
| Readability  | Good for one layer       | Better for pipelines                |

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                        | Recommendation          |
| ------------------------------- | ----------------------- |
| One quick aggregate then filter | Derived table           |
| Same intermediate used twice    | CTE                     |
| Need predictable column names   | Alias inner expressions |
| Need readable qualification     | Alias the relation too  |
