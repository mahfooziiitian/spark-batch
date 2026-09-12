# :material-null: NULL in Comparison Operators

In Spark 4.2, standard comparison operators return `NULL` whenever a comparison depends on an unknown value.

______________________________________________________________________

## :material-sitemap: Overview

| Pattern                          | Verified result |
| -------------------------------- | --------------- |
| `NULL = NULL`                    | `NULL`          |
| `5 = NULL`                       | `NULL`          |
| `NULL <=> NULL`                  | `TRUE`          |
| `5 <=> NULL`                     | `FALSE`         |
| `NULL IS NULL`                   | `TRUE`          |
| `NULL IS NOT DISTINCT FROM NULL` | `TRUE`          |

### :material-animation-play: Interactive Visualization — Comparison Outcomes

<div id="viz-null-comparison" class="ts-viz"></div>

Choose a nullable comparison to see whether Spark 4.2 returns `TRUE`, `FALSE`, or `NULL`. The visualization highlights the cases where null-safe comparison changes the outcome.

______________________________________________________________________

## :material-table: Standard vs Null-Safe Comparison

| Operator                  | NULL behavior                                                               |
| ------------------------- | --------------------------------------------------------------------------- |
| `=` / `<>` / `!=`         | Returns `NULL` if either side is NULL                                       |
| `<` / `<=` / `>` / `>=`   | Returns `NULL` if either side is NULL                                       |
| `<=>`                     | Returns `TRUE` when both sides are NULL, `FALSE` when only one side is NULL |
| `IS NULL` / `IS NOT NULL` | Tests presence or absence of a value                                        |
| `IS [NOT] DISTINCT FROM`  | ANSI null-aware comparison form supported by Spark 4.2                      |

______________________________________________________________________

## :material-magnify: Verified Behavior

### Standard comparisons do not treat `NULL` as equal or unequal

```sql
SELECT
    NULL = NULL AS eq_null,
    5 = NULL AS eq_mixed,
    NULL <> NULL AS ne_null,
    NULL > 5 AS gt_null;
```

```text
+-------+--------+-------+-------+
|eq_null|eq_mixed|ne_null|gt_null|
+-------+--------+-------+-------+
|NULL   |NULL    |NULL   |NULL   |
+-------+--------+-------+-------+
```

### Null-safe equality

```sql
SELECT
    NULL <=> NULL AS nseq_null,
    5 <=> NULL AS nseq_mixed,
    5 <=> 5 AS nseq_equal,
    5 <=> 6 AS nseq_not_equal;
```

```text
+---------+----------+----------+--------------+
|nseq_null|nseq_mixed|nseq_equal|nseq_not_equal|
+---------+----------+----------+--------------+
|true     |false     |true      |false         |
+---------+----------+----------+--------------+
```

### ANSI spelling with `IS [NOT] DISTINCT FROM`

```sql
SELECT
    NULL IS NOT DISTINCT FROM NULL AS indf_null,
    5 IS NOT DISTINCT FROM NULL AS indf_mixed,
    5 IS DISTINCT FROM NULL AS idf_mixed;
```

```text
+---------+----------+---------+
|indf_null|indf_mixed|idf_mixed|
+---------+----------+---------+
|true     |false     |true     |
+---------+----------+---------+
```

### Why `= NULL` never works in `WHERE`

```sql
SELECT *
FROM
VALUES
    ('Joe', 30),
    ('Marry', CAST(NULL AS INT))
AS person(name, age)
WHERE age = NULL;
```

```text
+----+---+
|name|age|
+----+---+
+----+---+
```

Use `age IS NULL` instead.

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- Use `IS NULL` and `IS NOT NULL` for null checks.
- Use `<=>` or `IS NOT DISTINCT FROM` when NULLs should compare equal.
- Expect ordinary comparisons with NULLs to return `NULL`, not `FALSE`.

<script src="../../assets/js/querying-nulls-viz.js"></script>
