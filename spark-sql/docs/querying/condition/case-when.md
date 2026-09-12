# :material-code-braces: CASE WHEN

`CASE WHEN` is Spark SQL's main conditional expression. It evaluates branches in order and returns the result from the first matching branch.

## :material-animation-play: Interactive Visualization — CASE Branch Resolver

<div id="viz-case-when" class="ts-viz"></div>

Switch between simple and searched `CASE` forms to see how branch order, `NULL`, and the optional `ELSE` affect the final value.

<script src="../../../assets/js/querying-condition-viz.js"></script>

______________________________________________________________________

## :material-pin: Syntax

### Searched CASE

```sql
CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ELSE default_result
END
```

### Simple CASE

```sql
CASE expression
    WHEN value1 THEN result1
    WHEN value2 THEN result2
    ELSE default_result
END
```

______________________________________________________________________

## :material-magnify: Verified Behavior

| Behavior checked in PySpark 4.2                      | Result                                                              |
| ---------------------------------------------------- | ------------------------------------------------------------------- |
| `CASE WHEN 1 = 2 THEN 'x' END`                       | Returns `NULL` because `ELSE` is implicit                           |
| `CASE NULL WHEN NULL THEN 'x' ELSE 'y' END`          | Returns `'y'`; simple `CASE` does not treat `NULL = NULL` as `TRUE` |
| `CASE WHEN NULL IS NULL THEN 'x' ELSE 'y' END`       | Returns `'x'`                                                       |
| `CASE WHEN true THEN 1 ELSE raise_error('boom') END` | Returns `1`; the unused branch is not evaluated                     |
| `CASE WHEN NULL THEN 'x' ELSE 'y' END`               | Fails analysis because bare `NULL` is `VOID`, not `BOOLEAN`         |

!!! note "Implicit NULL"

    If no `WHEN` matches and you omit `ELSE`, Spark returns `NULL`. That is true even when every `THEN` branch is non-null.

!!! note "Branch conditions must be boolean"

    If you need an explicit unknown condition for a searched `CASE`, cast it: `CAST(NULL AS BOOLEAN)`.
    In PySpark 4.2, `CASE WHEN CAST(NULL AS BOOLEAN) THEN 'x' ELSE 'y' END` returned `'y'`.

______________________________________________________________________

## :material-table: Searched vs Simple CASE

| Feature                               | Searched `CASE`  | Simple `CASE`    |
| ------------------------------------- | ---------------- | ---------------- |
| Arbitrary predicates                  | :material-check: | :material-close: |
| Range checks                          | :material-check: | :material-close: |
| Equality-only mapping                 | :material-check: | :material-check: |
| Explicit `IS NULL` handling           | :material-check: | :material-close: |
| Best for finite code-to-label mapping | :material-close: | :material-check: |

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Tier classification

```sql
SELECT
    customer_id,
    total_spend,
    CASE
        WHEN total_spend >= 10000 THEN 'Platinum'
        WHEN total_spend >= 5000 THEN 'Gold'
        WHEN total_spend >= 1000 THEN 'Silver'
        ELSE 'Standard'
    END AS tier
FROM customers;
```

### NULL-aware labelling

```sql
SELECT
    user_id,
    email,
    CASE
        WHEN email IS NULL THEN 'missing'
        WHEN email LIKE '%test%' THEN 'test account'
        ELSE 'valid'
    END AS email_status
FROM users;
```

### Conditional aggregation

```sql
SELECT
    region,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN status = 'shipped' THEN 1 ELSE 0 END) AS shipped,
    SUM(CASE WHEN status = 'returned' THEN 1 ELSE 0 END) AS returned
FROM orders
GROUP BY region;
```

!!! tip "FILTER can be cleaner"

    ```sql
    COUNT(*) FILTER (WHERE status = 'returned') AS returned
    ```

    Use this when you want a conditional count and do not need custom return values.

### Custom sort order

```sql
SELECT *
FROM users
ORDER BY
    CASE
        WHEN is_active = TRUE THEN 0
        WHEN is_active = FALSE THEN 1
        ELSE 2
    END,
    user_id;
```

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                         | Why it is wrong                                                      | Safer pattern                                     |
| ------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------- |
| `CASE col WHEN NULL THEN ...`   | Simple `CASE` compares with `=`; `NULL = NULL` is `NULL`, not `TRUE` | Use searched `CASE` with `WHEN col IS NULL`       |
| Omitting `ELSE` unintentionally | Unmatched rows become `NULL`                                         | Add an explicit default branch                    |
| Putting broad conditions first  | First match wins; later branches are skipped                         | Order branches from most specific to most general |
| Deeply nested `CASE`            | Hard to read and test                                                | Flatten branches or join to a lookup table        |

______________________________________________________________________

## :material-brain: When to Use

| Scenario                        | Pattern                                       |
| ------------------------------- | --------------------------------------------- |
| Multi-branch row classification | Searched `CASE`                               |
| Code-to-label mapping           | Simple `CASE`                                 |
| Conditional aggregation         | `CASE` inside `SUM`, `COUNT`, or `AVG`        |
| Custom ordering or bucketing    | `CASE` in `ORDER BY` or `GROUP BY`            |
| Nullable inputs                 | Searched `CASE` with explicit `IS NULL` logic |
