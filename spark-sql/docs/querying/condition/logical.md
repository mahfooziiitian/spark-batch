# :material-gate-and: Logical Conditions

Logical operators combine or invert predicates to build larger boolean expressions.
Spark SQL uses three-valued logic, so every nullable boolean can be `TRUE`, `FALSE`, or `NULL`.

## :material-animation-play: Interactive Visualization — Three-Valued Logic Explorer

<div id="viz-logical" class="ts-viz"></div>

Pick operand values and watch `AND`, `OR`, and `NOT` update live, including the cases where `NULL` propagates and the cases where one operand already decides the result.

<script src="../../../assets/js/querying-condition-viz.js"></script>

______________________________________________________________________

## :material-pin: Operator Reference

| Operator     | Syntax                  | Description                                       |
| ------------ | ----------------------- | ------------------------------------------------- |
| `AND`        | `A AND B`               | `TRUE` only when both operands are `TRUE`         |
| `OR`         | `A OR B`                | `TRUE` when at least one operand is `TRUE`        |
| `NOT`        | `NOT A`                 | Flips `TRUE` and `FALSE`; `NOT NULL` stays `NULL` |
| `EXISTS`     | `EXISTS (subquery)`     | `TRUE` when the subquery returns at least one row |
| `NOT EXISTS` | `NOT EXISTS (subquery)` | `TRUE` when the subquery returns no rows          |

______________________________________________________________________

## :material-table: Verified Truth Tables

### `AND`

| A       | B       | `A AND B` |
| ------- | ------- | --------- |
| `TRUE`  | `TRUE`  | `TRUE`    |
| `TRUE`  | `FALSE` | `FALSE`   |
| `TRUE`  | `NULL`  | `NULL`    |
| `FALSE` | `FALSE` | `FALSE`   |
| `FALSE` | `NULL`  | `FALSE`   |
| `NULL`  | `NULL`  | `NULL`    |

### `OR`

| A       | B       | `A OR B` |
| ------- | ------- | -------- |
| `TRUE`  | `TRUE`  | `TRUE`   |
| `TRUE`  | `FALSE` | `TRUE`   |
| `TRUE`  | `NULL`  | `TRUE`   |
| `FALSE` | `FALSE` | `FALSE`  |
| `FALSE` | `NULL`  | `NULL`   |
| `NULL`  | `NULL`  | `NULL`   |

### `NOT`

| A       | `NOT A` |
| ------- | ------- |
| `TRUE`  | `FALSE` |
| `FALSE` | `TRUE`  |
| `NULL`  | `NULL`  |

!!! note "Verified PySpark 4.2 examples"

    `FALSE AND NULL` returned `FALSE`, `TRUE OR NULL` returned `TRUE`, and `NULL AND NULL` returned `NULL`.

______________________________________________________________________

## :material-speedometer: Short-Circuit Cases

PySpark 4.2 was checked with `raise_error('boom')` to see when Spark can avoid evaluating the other side.

| Expression checked in PySpark 4.2 | Result           |
| --------------------------------- | ---------------- |
| `FALSE AND raise_error('boom')`   | Returns `FALSE`  |
| `TRUE OR raise_error('boom')`     | Returns `TRUE`   |
| `TRUE AND raise_error('boom')`    | Raises the error |
| `FALSE OR raise_error('boom')`    | Raises the error |

!!! warning "Do not treat predicates as control flow"

    These literal examples show Spark can short-circuit when one side already fixes the result.
    In real queries, the optimizer may rewrite predicates, so use this as a semantic aid, not as a side-effect programming technique.

______________________________________________________________________

## :material-sort-ascending: Operator Precedence

Highest to lowest:

1. `NOT`
2. `AND`
3. `OR`

```sql
WHERE (country = 'US' OR country = 'CA')
  AND is_active = TRUE
```

Without parentheses, `A AND B OR C` means `(A AND B) OR C`.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Combine requirements with `AND`

```sql
SELECT *
FROM orders
WHERE status = 'shipped'
  AND amount > 100
  AND order_date >= DATE '2024-01-01';
```

### Allow alternatives with `OR`

```sql
SELECT *
FROM users
WHERE tier IN ('gold', 'platinum', 'diamond');
```

### Exclude patterns with `NOT`

```sql
SELECT *
FROM events
WHERE NOT (event_type = 'test' OR event_type = 'debug');
```

### Semi-join with `EXISTS`

```sql
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

### Anti-join with `NOT EXISTS`

```sql
SELECT c.*
FROM customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.customer_id
      AND o.order_date >= current_date() - INTERVAL 90 DAYS
);
```

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                                   | Problem                                         | Fix                                                                                 |
| ----------------------------------------- | ----------------------------------------------- | ----------------------------------------------------------------------------------- |
| `WHERE NULL`                              | Bare `NULL` has type `VOID`, not `BOOLEAN`      | Use a boolean expression or `CAST(NULL AS BOOLEAN)` if you need an explicit unknown |
| Mixing `AND` and `OR` without parentheses | Easy to read incorrectly                        | Parenthesize OR groups explicitly                                                   |
| `NOT IN` with nullable subqueries         | `NULL` can turn the whole predicate into `NULL` | Prefer `NOT EXISTS`                                                                 |
| `WHERE flag = NULL`                       | Comparison result is `NULL`, not `TRUE`         | `WHERE flag IS NULL`                                                                |

______________________________________________________________________

## :material-brain: When to Use

| Scenario                          | Pattern                  |
| --------------------------------- | ------------------------ |
| Every condition must hold         | `AND`                    |
| Any one condition is enough       | `OR`                     |
| Invert a condition                | `NOT`                    |
| Keep rows with related matches    | `EXISTS`                 |
| Exclude rows with related matches | `NOT EXISTS`             |
| Mixed boolean logic               | Use explicit parentheses |
