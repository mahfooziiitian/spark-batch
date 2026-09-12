# :material-compare: Comparison Operators

Comparison operators evaluate expressions to `TRUE`, `FALSE`, or `NULL`. Spark SQL 4.2 follows three-valued logic for standard comparisons and adds null-safe forms for cases where `NULL` must participate explicitly.

## :material-animation-play: Interactive Visualization — Comparison Result Lab

<div id="viz-operator-comparison-null" class="ts-viz"></div>

Choose a verified Spark 4.2 expression to compare ordinary comparisons, null-safe equality, and pattern operators.

<script src="../../../assets/js/querying-operator-viz.js"></script>

______________________________________________________________________

## :material-code-tags: Syntax

| Operator                   | Meaning               | Verified NULL behavior                                            |
| -------------------------- | --------------------- | ----------------------------------------------------------------- |
| `=`                        | Equal                 | `NULL = NULL` returns `NULL`                                      |
| `!=` / `<>`                | Not equal             | `NULL <> NULL` returns `NULL`                                     |
| `>` `<` `>=` `<=`          | Ordering              | Any `NULL` operand returns `NULL`                                 |
| `<=>`                      | Null-safe equal       | `NULL <=> NULL` returns `TRUE`                                    |
| `IS DISTINCT FROM`         | Null-aware inequality | Returns `TRUE` when values differ, including `NULL` vs non-`NULL` |
| `IS NOT DISTINCT FROM`     | Null-aware equality   | Equivalent to `<=>`                                               |
| `BETWEEN ... AND ...`      | Inclusive range       | `NULL BETWEEN a AND b` returns `NULL`                             |
| `LIKE` / `ILIKE` / `RLIKE` | Pattern comparisons   | `NULL` input or `NULL` pattern returns `NULL`                     |

______________________________________________________________________

## :material-table: Verified Spark 4.2 Outcomes

| Expression checked in PySpark 4.2 | Result  |
| --------------------------------- | ------- |
| `NULL = NULL`                     | `NULL`  |
| `NULL <> NULL`                    | `NULL`  |
| `NULL <=> NULL`                   | `TRUE`  |
| `1 <=> NULL`                      | `FALSE` |
| `1 IS DISTINCT FROM NULL`         | `TRUE`  |
| `NULL IS DISTINCT FROM NULL`      | `FALSE` |
| `NULL IS NOT DISTINCT FROM NULL`  | `TRUE`  |
| `7 BETWEEN 10 AND 5`              | `FALSE` |
| `NULL BETWEEN 10 AND 5`           | `NULL`  |
| `'Abc' LIKE 'a%'`                 | `FALSE` |
| `'Abc' ILIKE 'a%'`                | `TRUE`  |
| `'abc' RLIKE '^[a-z]+$'`          | `TRUE`  |
| `'abc' LIKE NULL`                 | `NULL`  |

______________________________________________________________________

## :material-information-outline: Behavior Notes

1. Standard comparison operators never treat `NULL` as equal to anything, including another `NULL`.
2. `<=>` and `IS NOT DISTINCT FROM` are the boolean-safe equality choices for nullable data.
3. `IS DISTINCT FROM` is the null-safe inequality form; `NOT (a <=> b)` is equivalent.
4. `BETWEEN` is inclusive and respects operand order. Reversing the bounds changes the result.
5. Spark SQL 4.2 supports both default backslash escaping and an explicit `ESCAPE` clause in `LIKE` patterns.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### Ordinary vs null-safe equality

```sql
SELECT
    NULL = NULL AS ordinary_eq,
    NULL <=> NULL AS null_safe_eq,
    NULL IS NOT DISTINCT FROM NULL AS not_distinct;
```

### Null-aware inequality

```sql
SELECT
    1 IS DISTINCT FROM NULL AS different_values,
    NULL IS DISTINCT FROM NULL AS same_unknowns;
```

### Inclusive range checks

```sql
SELECT
    amount,
    amount BETWEEN 100 AND 500 AS in_band
FROM orders;
```

### Pattern comparisons

```sql
SELECT
    name LIKE 'Pro%' AS starts_with_pro,
    name ILIKE 'spark%' AS starts_with_spark_ignore_case,
    email RLIKE '^[^@]+@[^@]+\\.[^@]+$' AS looks_like_email
FROM contacts;
```

### Escaping wildcard characters

```sql
SELECT
    '100% complete' LIKE '100\% complete' AS default_escape,
    '100% complete' LIKE '100!% complete' ESCAPE '!' AS explicit_escape;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                        | Recommended pattern             |
| ------------------------------- | ------------------------------- |
| Non-nullable equality           | `=`                             |
| Nullable equality               | `<=>` or `IS NOT DISTINCT FROM` |
| Nullable inequality             | `IS DISTINCT FROM`              |
| Inclusive value band            | `BETWEEN ... AND ...`           |
| Case-insensitive wildcard match | `ILIKE`                         |
| Regex validation                | `RLIKE`                         |

!!! warning "`= NULL` still never works"

    `WHERE col = NULL` evaluates to `NULL`, not `TRUE`, so it filters out every row. Use `IS NULL` or `<=> NULL` instead.
