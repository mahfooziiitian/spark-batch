# :material-null: Null-Safe Operators

Null-safe operators and expressions let you compare nullable values deliberately instead of inheriting SQL's default `NULL` propagation rules.

## :material-animation-play: Interactive Visualization — Nullable Equality Matrix

<div id="viz-operator-null-safe" class="ts-viz"></div>

Pick a comparison form to see how Spark 4.2 treats value/value, value/`NULL`, and `NULL`/`NULL` cases.

<script src="../../../assets/js/querying-operator-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Syntax

| Expression                 | Purpose                          | Verified behavior                         |
| -------------------------- | -------------------------------- | ----------------------------------------- |
| `col IS NULL`              | Test for `NULL`                  | `NULL IS NULL` is `TRUE`                  |
| `col IS NOT NULL`          | Test for non-`NULL`              | `NULL IS NOT NULL` is `FALSE`             |
| `a <=> b`                  | Null-safe equality               | `NULL <=> NULL` is `TRUE`                 |
| `a IS DISTINCT FROM b`     | Null-safe inequality             | `1 IS DISTINCT FROM NULL` is `TRUE`       |
| `a IS NOT DISTINCT FROM b` | ANSI-style null-safe equality    | Equivalent to `<=>`                       |
| `COALESCE(a, b, ...)`      | First non-`NULL` value           | `COALESCE(NULL, NULL, 'x')` returns `'x'` |
| `NVL(a, b)`                | Two-argument alias of `COALESCE` | `NVL(NULL, 'y')` returns `'y'`            |
| `NULLIF(a, b)`             | Return `NULL` when values match  | `NULLIF(1, 1)` returns `NULL`             |

______________________________________________________________________

## :material-table: Verified Spark 4.2 Outcomes

| Expression checked in PySpark 4.2 | Result  |
| --------------------------------- | ------- |
| `NULL IS NULL`                    | `TRUE`  |
| `1 IS NOT NULL`                   | `TRUE`  |
| `NULL IS NOT NULL`                | `FALSE` |
| `5 <=> NULL`                      | `FALSE` |
| `NULL <=> NULL`                   | `TRUE`  |
| `1 IS DISTINCT FROM NULL`         | `TRUE`  |
| `NULL IS DISTINCT FROM NULL`      | `FALSE` |
| `NULL IS NOT DISTINCT FROM NULL`  | `TRUE`  |
| `COALESCE(NULL, NULL, 'x')`       | `'x'`   |
| `NVL(NULL, 'y')`                  | `'y'`   |
| `NULLIF(1, 1)`                    | `NULL`  |
| `NULLIF(1, 2)`                    | `1`     |
| `1 / NULLIF(0, 0)`                | `NULL`  |

______________________________________________________________________

## :material-information-outline: Behavior Notes

1. `IS NULL` and `IS NOT NULL` are the direct tests for nullness.
2. `<=>` and `IS NOT DISTINCT FROM` are the equality forms that never return `NULL`.
3. `IS DISTINCT FROM` stays boolean as well, making it convenient for change detection.
4. `NULLIF` is a practical zero-divisor guard because `a / NULLIF(b, 0)` becomes `a / NULL` when `b = 0`.
5. `NVL` is available in Spark SQL 4.2, but `COALESCE` scales better when you need more than two fallback expressions.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Nullable equality in joins

```sql
SELECT *
FROM left_table AS l
JOIN right_table AS r
    ON l.optional_key <=> r.optional_key;
```

### Null-safe inequality

```sql
SELECT *
FROM customer_changes
WHERE old_region IS DISTINCT FROM new_region;
```

### Fallback values

```sql
SELECT
    customer_id,
    COALESCE(display_name, legal_name, username) AS preferred_name,
    NVL(country_code, 'unknown') AS country_code
FROM customers;
```

### Divide-by-zero guard

```sql
SELECT
    revenue,
    units_sold,
    revenue / NULLIF(units_sold, 0) AS revenue_per_unit
FROM sales_summary;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                             | Recommended pattern             |
| ------------------------------------ | ------------------------------- |
| Find actual `NULL`s                  | `IS NULL` / `IS NOT NULL`       |
| Join nullable keys                   | `<=>` or `IS NOT DISTINCT FROM` |
| Detect nullable changes              | `IS DISTINCT FROM`              |
| Provide defaults                     | `COALESCE` / `NVL`              |
| Turn a matching sentinel into `NULL` | `NULLIF`                        |

!!! warning "Avoid `!= NULL` as well as `= NULL`"

    Both expressions evaluate to `NULL`, not to `TRUE` or `FALSE`. Use `IS NULL`, `IS NOT NULL`, or a null-safe comparison instead.
