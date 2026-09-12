# :material-numeric-1-box-outline: Scalar Subqueries

A scalar subquery is a subquery used where Spark expects a single value. PySpark 4.2 accepts it only when the subquery produces exactly one column, and at execution time no more than one row.

<script src="../../assets/js/querying-subquery-viz.js"></script>

### :material-animation-play: Interactive Visualization — Scalar Cardinality Outcomes

<div id="viz-subquery-scalar-cases" class="ts-viz"></div>

Toggle among the four cardinality cases to see which ones succeed and which exact Spark 4.2 errors appear.

______________________________________________________________________

## :material-code-tags: Valid Placements

```sql
SELECT (SELECT MAX(amount) FROM orders) AS max_amount;
```

```sql
SELECT order_id
FROM orders
WHERE amount > (SELECT AVG(amount) FROM orders);
```

```sql
SELECT
    customer,
    SUM(amount) AS total_spent
FROM orders
GROUP BY customer
HAVING SUM(amount) > (SELECT AVG(amount) FROM orders);
```

______________________________________________________________________

## :material-information-outline: Verified Spark 4.2 Rules

1. **One column only.** A scalar subquery with two output columns fails analysis with `INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN`.
2. **At most one row.** If execution produces multiple rows, Spark raises `SCALAR_SUBQUERY_TOO_MANY_ROWS` with the message `More than one row returned by a subquery used as an expression.`
3. **Zero rows becomes `NULL`.** `SELECT (SELECT customer FROM orders WHERE country = 'ZZ')` returns `NULL`.
4. **Uncorrelated scalar subqueries are not join rewrites.** In the physical plan Spark keeps a `Subquery` node and evaluates it once.
5. **Correlated scalar subqueries can be decorrelated.** A correlated aggregate filter such as `amount > (SELECT AVG(...) ...)` is rewritten to a join in Spark 4.2.

______________________________________________________________________

## :material-flask-outline: Verified Examples

### Global threshold

```sql
SELECT order_id, amount
FROM orders
WHERE amount > (SELECT AVG(amount) FROM orders)
ORDER BY order_id;
```

With the sample data used during verification, Spark returned order ids `1`, `3`, and `5`.

### Zero-row scalar result

```sql
SELECT (SELECT customer FROM orders WHERE country = 'ZZ') AS no_customer;
```

Result:

```text
+-----------+
|no_customer|
+-----------+
|       NULL|
+-----------+
```

### Multiple-row runtime failure

```sql
SELECT (SELECT customer FROM orders WHERE country = 'US') AS any_customer;
```

Spark 4.2 raises:

```text
[SCALAR_SUBQUERY_TOO_MANY_ROWS] More than one row returned by a subquery used as an expression.
```

### Multiple-column analysis failure

```sql
SELECT (SELECT order_id, amount FROM orders LIMIT 1) AS pair;
```

Spark 4.2 raises:

```text
[INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN]
Invalid subquery: Scalar subquery must return only one column, but got 2.
```

______________________________________________________________________

## :material-swap-horizontal: Plan Shape

| Query shape                                       | Spark 4.2 plan shape                 |
| ------------------------------------------------- | ------------------------------------ |
| `WHERE amount > (SELECT AVG(amount) FROM orders)` | `Filter` with a `Subquery` child     |
| Correlated aggregate in `WHERE`                   | Join rewrite                         |
| Correlated scalar in `SELECT` list                | Typically a `LeftOuter` join rewrite |

______________________________________________________________________

## :material-lightbulb-outline: When to Use Scalar Subqueries

| Scenario                               | Recommendation               |
| -------------------------------------- | ---------------------------- |
| Compare to one global aggregate        | Good fit                     |
| Pull one config value                  | Good fit                     |
| Correlated per-key lookup              | Valid, but inspect `EXPLAIN` |
| Anything that can return multiple rows | Add an aggregate or rewrite  |

!!! tip "Prefer deterministic single-row logic"

    `LIMIT 1` satisfies the one-row rule, but without `ORDER BY` it does not document which row you wanted. Prefer `MAX`, `MIN`, `AVG`, `COUNT`, or `ORDER BY ... LIMIT 1`.
