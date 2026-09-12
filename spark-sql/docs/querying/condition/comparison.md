# :material-compare: Comparison Conditions

Comparison predicates test equality, ordering, range membership, or set membership between values.
They are the building blocks behind most `WHERE`, `HAVING`, and `JOIN ... ON` clauses.

## :material-animation-play: Interactive Visualization — NULL-Aware Comparison Lab

<div id="viz-comparison" class="ts-viz"></div>

Toggle operand values and operators to compare ordinary equality with Spark's null-aware forms such as `<=>` and `IS DISTINCT FROM`.

<script src="../../../assets/js/querying-condition-viz.js"></script>

______________________________________________________________________

## :material-pin: Operator Reference

| Operator               | Syntax                     | Notes                                                    |
| ---------------------- | -------------------------- | -------------------------------------------------------- |
| `=`                    | `a = b`                    | Standard equality; `NULL` operands produce `NULL`        |
| `!=` / `<>`            | `a <> b`                   | Standard inequality; `NULL` operands produce `NULL`      |
| `>` `>=` `<` `<=`      | `a >= 10`                  | Ordering comparisons                                     |
| `<=>`                  | `a <=> b`                  | Null-safe equality; equivalent to `IS NOT DISTINCT FROM` |
| `BETWEEN ... AND`      | `x BETWEEN 1 AND 5`        | Inclusive on both ends                                   |
| `NOT BETWEEN`          | `x NOT BETWEEN 1 AND 5`    | Negated inclusive range                                  |
| `IN (...)`             | `id IN (1, 2, 3)`          | Set membership                                           |
| `NOT IN (...)`         | `id NOT IN (1, 2, 3)`      | Be careful with `NULL` in the list or subquery           |
| `IS DISTINCT FROM`     | `a IS DISTINCT FROM b`     | Null-aware inequality                                    |
| `IS NOT DISTINCT FROM` | `a IS NOT DISTINCT FROM b` | Null-aware equality                                      |

______________________________________________________________________

## :material-table: Verified NULL Semantics

| Expression checked in PySpark 4.2 | Result  |
| --------------------------------- | ------- |
| `NULL = NULL`                     | `NULL`  |
| `NULL <> NULL`                    | `NULL`  |
| `NULL <=> NULL`                   | `TRUE`  |
| `1 <=> NULL`                      | `FALSE` |
| `NULL IS DISTINCT FROM NULL`      | `FALSE` |
| `1 IS DISTINCT FROM NULL`         | `TRUE`  |
| `NULL IS NOT DISTINCT FROM NULL`  | `TRUE`  |
| `NULL IN (1, 2, NULL)`            | `NULL`  |
| `1 NOT IN (2, NULL)`              | `NULL`  |
| `2 NOT IN (2, NULL)`              | `FALSE` |

!!! warning "`NOT IN` and NULL"

    If the right-hand list or subquery can contain `NULL`, `NOT IN` can return `NULL` instead of `TRUE`.
    In a `WHERE` clause, that means rows disappear. Prefer `NOT EXISTS` or filter `NULL` out of the subquery.

______________________________________________________________________

## :material-tune: ANSI Mode and Type Coercion

PySpark 4.2 kept the null-comparison truth tables the same in both ANSI modes, but invalid implicit casts behaved differently.

| Query checked in PySpark 4.2 | ANSI off | ANSI on                         |
| ---------------------------- | -------- | ------------------------------- |
| `SELECT '1' = 1`             | `TRUE`   | `TRUE`                          |
| `SELECT 'abc' = 1`           | `NULL`   | Error: invalid cast to `BIGINT` |

!!! note "Practical takeaway"

    ANSI mode matters most when Spark has to coerce incompatible types. For predictable behavior, cast explicitly instead of relying on implicit conversion.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Range filter with `BETWEEN`

```sql
SELECT *
FROM sessions
WHERE duration BETWEEN 60 AND 300;
```

### Membership with `IN`

```sql
SELECT *
FROM users
WHERE country IN ('US', 'CA', 'UK');
```

### Safe anti-join pattern

```sql
SELECT u.*
FROM users u
WHERE NOT EXISTS (
    SELECT 1
    FROM blocked b
    WHERE b.user_id = u.user_id
);
```

### NULL-safe join condition

```sql
SELECT *
FROM a
JOIN b
ON a.device_id <=> b.device_id;
```

### Change detection including `NULL` transitions

```sql
SELECT *
FROM dim_customer
WHERE new_city IS DISTINCT FROM old_city;
```

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                                  | Problem                                                     | Fix                                                             |
| ---------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------- |
| `WHERE col = NULL`                       | Never evaluates to `TRUE`                                   | `WHERE col IS NULL`                                             |
| `NOT IN (subquery)` without a null guard | Rows can vanish unexpectedly                                | Use `NOT EXISTS` or `WHERE col IS NOT NULL` inside the subquery |
| `BETWEEN` on timestamp end dates         | Inclusive end points often miss later times on the same day | Prefer `>= start AND < next_day`                                |
| Comparing unlike types casually          | Behavior changes under ANSI mode                            | Cast explicitly                                                 |

______________________________________________________________________

## :material-brain: When to Use

| Scenario                                  | Pattern                         |
| ----------------------------------------- | ------------------------------- |
| Equality with no null matching            | `=`                             |
| Equality where `NULL = NULL` should match | `<=>` or `IS NOT DISTINCT FROM` |
| Detecting any value change                | `IS DISTINCT FROM`              |
| Inclusive numeric or date range           | `BETWEEN`                       |
| Exclusion with possible `NULL`s           | `NOT EXISTS`                    |
