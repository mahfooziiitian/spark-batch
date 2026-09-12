# :material-call-split: IF, IIF, and Null-Handling Functions

Spark SQL includes compact conditional functions for two-branch logic and null handling.
They are convenient, but their behavior around `NULL`, branch evaluation, and portability matters.

## :material-animation-play: Interactive Visualization — Conditional Function Explorer

<div id="viz-if-iif" class="ts-viz"></div>

Switch between `IF`, `IFNULL`, `NULLIF`, and `COALESCE` to see which argument wins and when Spark stops evaluating later arguments.

<script src="../../../assets/js/querying-condition-viz.js"></script>

______________________________________________________________________

## :material-pin: Function Reference

| Function           | Syntax                               | What PySpark 4.2 verified                                                  |
| ------------------ | ------------------------------------ | -------------------------------------------------------------------------- |
| `IF`               | `IF(cond, true_val, false_val)`      | Built in; `IF(NULL, a, b)` returns `b`                                     |
| `[Databricks] IIF` | `IIF(cond, true_val, false_val)`     | **Not available in open-source Spark 4.2**; `IIF(...)` is unresolved there |
| `IFNULL`           | `IFNULL(expr, replacement)`          | Returns the replacement only when `expr` is `NULL`                         |
| `NULLIF`           | `NULLIF(expr, comparand)`            | Returns `NULL` when both arguments compare equal                           |
| `COALESCE`         | `COALESCE(e1, e2, ...)`              | Returns the first non-null argument                                        |
| `NVL`              | `NVL(expr, replacement)`             | Alias for `IFNULL`                                                         |
| `NVL2`             | `NVL2(expr, not_null_val, null_val)` | Returns one value for non-null, another for null                           |

!!! warning "Portability"

    If the query must run on open-source Spark 4.2, use `IF`, not `IIF`.
    The PySpark 4.2 check for `SELECT IIF(true, 'yes', 'no')` failed with `UNRESOLVED_ROUTINE`.

______________________________________________________________________

## :material-magnify: Verified Evaluation Semantics

PySpark 4.2 was checked with `raise_error('boom')` in unused branches.

| Expression checked in PySpark 4.2        | Result                                                     |
| ---------------------------------------- | ---------------------------------------------------------- |
| `IF(true, 1, raise_error('boom'))`       | Returns `1`                                                |
| `IF(false, raise_error('boom'), 2)`      | Returns `2`                                                |
| `IFNULL(1, raise_error('boom'))`         | Returns `1`                                                |
| `IFNULL(NULL, raise_error('boom'))`      | Raises the error                                           |
| `COALESCE(1, raise_error('boom'))`       | Returns `1`                                                |
| `COALESCE(NULL, raise_error('boom'), 3)` | Raises the error                                           |
| `NULLIF(5, 5)`                           | Returns `NULL`                                             |
| `NULLIF(5, raise_error('boom'))`         | Raises the error because Spark must evaluate the comparand |

!!! note "Short-circuit vs eager evaluation"

    `IF`, `IFNULL`, and `COALESCE` skip unused arguments. `NULLIF` does not; Spark must evaluate both arguments to decide whether they are equal.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### `IF` for simple two-way branching

```sql
SELECT
    order_id,
    amount,
    IF(amount >= 100, 'high_value', 'standard') AS value_band
FROM orders;
```

### `[Databricks] IIF`

```sql
SELECT
    user_id,
    IIF(is_active, 'active', 'inactive') AS status
FROM users;
```

!!! note "[Databricks]"

    Keep `IIF` examples clearly labeled. Use `IF` when the same page should remain runnable on open-source Spark.

### `IFNULL` and `NVL`

```sql
SELECT
    customer_id,
    IFNULL(phone, 'N/A') AS phone,
    NVL(loyalty_points, 0) AS loyalty_points
FROM customers;
```

### `NULLIF` for divide-by-zero guards

```sql
SELECT
    product_id,
    revenue,
    units_sold,
    ROUND(revenue / NULLIF(units_sold, 0), 2) AS revenue_per_unit
FROM sales;
```

!!! note "ANSI-safe guard"

    `1 / NULLIF(0, 0)` returned `NULL` in PySpark 4.2 with ANSI both off and on, so this remains a safe divide-by-zero pattern.

### `COALESCE` for multiple fallbacks

```sql
SELECT
    user_id,
    COALESCE(preferred_name, display_name, username, 'Anonymous') AS resolved_name
FROM user_profiles;
```

### `NVL2` for null vs non-null branching

```sql
SELECT
    user_id,
    NVL2(phone, 'has phone', 'no phone') AS phone_status
FROM users;
```

______________________________________________________________________

## :material-compare: Choosing the Right Function

| Need                                        | Best choice       |
| ------------------------------------------- | ----------------- |
| Two explicit branches                       | `IF`              |
| Many fallback sources                       | `COALESCE`        |
| Replace one nullable value with one default | `IFNULL` or `NVL` |
| Turn equal values into `NULL`               | `NULLIF`          |
| Distinguish null from non-null in one call  | `NVL2`            |
| Open-source Spark compatibility             | Avoid `IIF`       |

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                                        | Problem                                 | Fix                                                                       |
| ---------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------- |
| `IF(col = NULL, ...)`                          | `col = NULL` is `NULL`, not `TRUE`      | `IF(col IS NULL, ...)`                                                    |
| Assuming `IF(NULL, a, b)` returns `NULL`       | It returns the false branch             | Write the fallback you want in the third argument                         |
| Assuming `NULLIF` short-circuits like `IF`     | It evaluates both arguments             | Use `IF` or `CASE` when the second expression is expensive or error-prone |
| Using `IIF` in OSS Spark docs without labeling | The example will not run in PySpark 4.2 | Mark it `[Databricks]` or rewrite it with `IF`                            |
