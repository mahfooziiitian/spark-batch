# :material-null: NULL in Logical Operators

Spark 4.2 uses three-valued logic: boolean expressions can evaluate to `TRUE`, `FALSE`, or `NULL`.

______________________________________________________________________

## :material-sitemap: Overview

A known `FALSE` is enough to determine `AND`, and a known `TRUE` is enough to determine `OR`. In every other nullable case, Spark returns `NULL`.

### :material-animation-play: Interactive Visualization — Three-Valued Logic

<div id="viz-null-logical" class="ts-viz"></div>

Pick operands and an operator to see the exact Spark 4.2 result. The result chip changes between `TRUE`, `FALSE`, and `NULL` as the truth table changes.

______________________________________________________________________

## :material-table: Truth Tables

### `AND`

| Left    | Right   | Result  |
| ------- | ------- | ------- |
| `TRUE`  | `TRUE`  | `TRUE`  |
| `TRUE`  | `FALSE` | `FALSE` |
| `TRUE`  | `NULL`  | `NULL`  |
| `FALSE` | `TRUE`  | `FALSE` |
| `FALSE` | `FALSE` | `FALSE` |
| `FALSE` | `NULL`  | `FALSE` |
| `NULL`  | `TRUE`  | `NULL`  |
| `NULL`  | `FALSE` | `FALSE` |
| `NULL`  | `NULL`  | `NULL`  |

### `OR`

| Left    | Right   | Result  |
| ------- | ------- | ------- |
| `TRUE`  | `TRUE`  | `TRUE`  |
| `TRUE`  | `FALSE` | `TRUE`  |
| `TRUE`  | `NULL`  | `TRUE`  |
| `FALSE` | `TRUE`  | `TRUE`  |
| `FALSE` | `FALSE` | `FALSE` |
| `FALSE` | `NULL`  | `NULL`  |
| `NULL`  | `TRUE`  | `TRUE`  |
| `NULL`  | `FALSE` | `NULL`  |
| `NULL`  | `NULL`  | `NULL`  |

### `NOT`

| Operand | Result  |
| ------- | ------- |
| `TRUE`  | `FALSE` |
| `FALSE` | `TRUE`  |
| `NULL`  | `NULL`  |

______________________________________________________________________

## :material-flask-outline: Verified Examples

```sql
SELECT
    TRUE OR NULL AS t_or_n,
    FALSE OR NULL AS f_or_n,
    TRUE AND NULL AS t_and_n,
    FALSE AND NULL AS f_and_n,
    NOT NULL AS not_n,
    NULL AND NULL AS n_and_n,
    NULL OR NULL AS n_or_n;
```

```text
+------+------+-------+-------+-----+-------+------+
|t_or_n|f_or_n|t_and_n|f_and_n|not_n|n_and_n|n_or_n|
+------+------+-------+-------+-----+-------+------+
|true  |NULL  |NULL   |false  |NULL |NULL   |NULL  |
+------+------+-------+-------+-----+-------+------+
```

### Why three-valued logic matters in filters

```sql
SELECT name
FROM
VALUES
    ('Joe', true),
    ('Marry', CAST(NULL AS BOOLEAN)),
    ('Mike', false)
AS t(name, flag)
WHERE NOT flag;
```

```text
+----+
|name|
+----+
|Mike|
+----+
```

`NOT NULL` is still `NULL`, so the `Marry` row is excluded.

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- `FALSE AND NULL` is `FALSE`.
- `TRUE OR NULL` is `TRUE`.
- `NOT NULL` stays `NULL`, so nullable booleans need careful filtering.

<script src="../../assets/js/querying-nulls-viz.js"></script>
