# :material-null: NULL Checks and Replacement Functions

Spark 4.2 provides dedicated predicates and functions for testing NULLs and substituting fallback values without relying on `= NULL`.

______________________________________________________________________

## :material-sitemap: Overview

| Construct                    | Verified behavior                             |
| ---------------------------- | --------------------------------------------- |
| `expr IS NULL`               | Returns `TRUE` only when `expr` is NULL       |
| `expr IS NOT NULL`           | Returns `TRUE` only when `expr` is not NULL   |
| `COALESCE(a, b, ...)`        | Returns the first non-NULL argument           |
| `NULLIF(a, b)`               | Returns `NULL` when `a = b`, else returns `a` |
| `IFNULL(a, b)` / `NVL(a, b)` | Returns `b` when `a` is NULL, else `a`        |
| `NVL2(a, b, c)`              | Returns `b` when `a` is not NULL, else `c`    |

### :material-animation-play: Interactive Visualization — NULL Check Helpers

<div id="viz-null-check" class="ts-viz"></div>

Switch between NULL-handling helpers to see the exact output Spark 4.2 returns for the selected inputs.

______________________________________________________________________

## :material-magnify: Verified Rules

- `expr = NULL` never returns `TRUE`.
- `IS NULL` and `IS NOT NULL` always return `TRUE` or `FALSE`, never `NULL`.
- `IFNULL` and `NVL` are two-argument fallback helpers; `COALESCE` extends the idea to more than two inputs.
- `NVL2` branches on whether its first argument is NULL.

______________________________________________________________________

## :material-flask-outline: Verified Examples

### `IS NULL` and `IS NOT NULL`

```sql
SELECT
    NULL IS NULL AS is_null_pred,
    NULL IS NOT NULL AS is_not_null_pred,
    ISNULL(NULL) AS isnull_fn,
    ISNOTNULL(1) AS isnotnull_fn;
```

```text
+------------+----------------+---------+-------------+
|is_null_pred|is_not_null_pred|isnull_fn|isnotnull_fn|
+------------+----------------+---------+-------------+
|true        |false           |true     |true         |
+------------+----------------+---------+-------------+
```

### `COALESCE`, `IFNULL`, and `NVL`

```sql
SELECT
    COALESCE(NULL, NULL, 7) AS coalesce_v,
    IFNULL(NULL, 'x') AS ifnull_v,
    NVL(NULL, 'y') AS nvl_v;
```

```text
+----------+--------+-----+
|coalesce_v|ifnull_v|nvl_v|
+----------+--------+-----+
|7         |x       |y    |
+----------+--------+-----+
```

### `NULLIF`

```sql
SELECT
    NULLIF(5, 5) AS nullif_eq,
    NULLIF(5, 6) AS nullif_ne;
```

```text
+---------+---------+
|nullif_eq|nullif_ne|
+---------+---------+
|NULL     |5        |
+---------+---------+
```

### `NVL2`

```sql
SELECT
    NVL2(NULL, 'yes', 'no') AS nvl2_null,
    NVL2('a', 'yes', 'no') AS nvl2_not_null;
```

```text
+---------+-------------+
|nvl2_null|nvl2_not_null|
+---------+-------------+
|no       |yes          |
+---------+-------------+
```

### Using `COALESCE` in a filter-friendly projection

```sql
SELECT
    name,
    COALESCE(age, -1) AS age_or_default
FROM
VALUES
    ('Joe', 30),
    ('Marry', CAST(NULL AS INT))
AS person(name, age);
```

```text
+-----+--------------+
|name |age_or_default|
+-----+--------------+
|Joe  |30            |
|Marry|-1            |
+-----+--------------+
```

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                           | Why it fails                                             | Better pattern          |
| --------------------------------- | -------------------------------------------------------- | ----------------------- |
| `WHERE col = NULL`                | Result is `NULL`, so the row is not kept                 | `WHERE col IS NULL`     |
| `WHERE col != NULL`               | Result is `NULL`, not `TRUE`                             | `WHERE col IS NOT NULL` |
| `NULLIF(a, b)` for null detection | It compares values; it does not test whether `a` is NULL | Use `a IS NULL`         |

<script src="../../assets/js/querying-nulls-viz.js"></script>
