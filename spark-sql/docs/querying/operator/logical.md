# :material-gate-and: Logical Operators

Logical operators combine boolean expressions. Spark SQL 4.2 uses three-valued logic, so `NULL` participates as an unknown truth value rather than being coerced to `FALSE` inside the expression itself.

## :material-animation-play: Interactive Visualization — Logical Precedence Lab

<div id="viz-operator-logical-precedence" class="ts-viz"></div>

Switch between verified expressions to see Spark 4.2 precedence rules and how `NULL` changes boolean outcomes.

<script src="../../../assets/js/querying-operator-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Syntax

```sql
WHERE condition_a AND condition_b
WHERE condition_a OR condition_b
WHERE NOT condition_a
WHERE (condition_a OR condition_b) AND NOT condition_c
```

______________________________________________________________________

## :material-table: Verified Truth Tables

### `AND`

| `A`     | `B`     | `A AND B` |
| ------- | ------- | --------- |
| `TRUE`  | `TRUE`  | `TRUE`    |
| `TRUE`  | `FALSE` | `FALSE`   |
| `TRUE`  | `NULL`  | `NULL`    |
| `FALSE` | `FALSE` | `FALSE`   |
| `FALSE` | `NULL`  | `FALSE`   |
| `NULL`  | `NULL`  | `NULL`    |

### `OR`

| `A`     | `B`     | `A OR B` |
| ------- | ------- | -------- |
| `TRUE`  | `TRUE`  | `TRUE`   |
| `TRUE`  | `FALSE` | `TRUE`   |
| `TRUE`  | `NULL`  | `TRUE`   |
| `FALSE` | `FALSE` | `FALSE`  |
| `FALSE` | `NULL`  | `NULL`   |
| `NULL`  | `NULL`  | `NULL`   |

### `NOT`

| `A`     | `NOT A` |
| ------- | ------- |
| `TRUE`  | `FALSE` |
| `FALSE` | `TRUE`  |
| `NULL`  | `NULL`  |

!!! note "Filtering with `WHERE`"

    A `WHERE` clause keeps only rows whose predicate is `TRUE`. Rows where the predicate is `FALSE` or `NULL` are excluded.

______________________________________________________________________

## :material-information-outline: Verified Precedence and Evaluation Notes

| Expression checked in PySpark 4.2        | Result  | What it shows                                      |
| ---------------------------------------- | ------- | -------------------------------------------------- |
| `TRUE OR FALSE AND FALSE`                | `TRUE`  | `AND` binds more tightly than `OR`                 |
| `(TRUE OR FALSE) AND FALSE`              | `FALSE` | Parentheses change the grouping                    |
| `NOT TRUE AND FALSE`                     | `FALSE` | `NOT` binds before `AND`                           |
| `NOT (TRUE AND FALSE)`                   | `TRUE`  | Parentheses can invert a full subexpression        |
| `FALSE AND (1 / 0 > 0)` with ANSI `true` | `FALSE` | Spark did not evaluate the RHS in this tested case |
| `TRUE OR (1 / 0 > 0)` with ANSI `true`   | `TRUE`  | Spark did not evaluate the RHS in this tested case |

!!! warning "Do not use short-circuit behavior as your safety guard"

    Spark 4.2 skipped the right-hand side in the tested `FALSE AND ...` and `TRUE OR ...` expressions, even with ANSI errors enabled. Still, unsafe arithmetic should be guarded explicitly with `NULLIF`, `try_divide`, or `CASE`, because Catalyst may rewrite larger predicates.

!!! note "This demo needs ANSI mode to prove anything"

    `1 / 0` only raises an error to *not* raise (proving short-circuiting) when ANSI mode is on. On
    a Databricks SQL warehouse (`stg` profile) with `ansi_mode = false` (verified with `SET -v`),
    `1 / 0` already returns `NULL` instead of erroring, so `FALSE AND (1 / 0 > 0)` and
    `TRUE OR (1 / 0 > 0)` both still evaluated to `FALSE` and `TRUE` there — but that no longer
    demonstrates short-circuiting, since a non-short-circuited evaluation would have produced the
    same result anyway. Re-run this check on a warehouse or cluster with ANSI mode enabled if you
    want to see the RHS actually being skipped.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Compound filters

```sql
SELECT *
FROM orders
WHERE status = 'ACTIVE'
  AND region = 'EU'
  AND amount > 100;
```

### Parentheses around `OR`

```sql
SELECT *
FROM orders
WHERE status = 'PENDING'
  AND (region = 'EU' OR region = 'APAC');
```

### `NULL` in a logical filter

```sql
SELECT *
FROM orders
WHERE discount = 0 OR discount IS NULL;
```

### Explicit divide-by-zero guard instead of relying on predicate order

```sql
SELECT *
FROM metrics
WHERE denominator IS NOT NULL
  AND numerator / NULLIF(denominator, 0) > 0.5;
```

### `CASE` with logical operators

```sql
SELECT
    customer_id,
    CASE
        WHEN is_vip AND total_spent >= 10000 THEN 'Platinum'
        WHEN is_vip OR total_spent >= 5000 THEN 'Gold'
        ELSE 'Standard'
    END AS tier
FROM customer_summary;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                  | Recommended pattern                                |
| ------------------------- | -------------------------------------------------- |
| All conditions must hold  | `AND`                                              |
| Any condition may hold    | `OR`                                               |
| Invert a boolean test     | `NOT`                                              |
| Mixed `AND` and `OR`      | Add parentheses                                    |
| Nullable filter inclusion | Combine the predicate with `IS NULL` or `COALESCE` |
