# :material-null: NULL in Filter Conditions

Spark 4.2 keeps rows only when a filter condition evaluates to `TRUE`; rows with `FALSE` or `NULL` results are excluded.

______________________________________________________________________

## :material-sitemap: Overview

| Clause                        | Verified NULL behavior                    |
| ----------------------------- | ----------------------------------------- |
| `WHERE`                       | Keeps only rows whose condition is `TRUE` |
| `HAVING`                      | Applies the same rule after aggregation   |
| `JOIN ... ON a.key = b.key`   | Does not match NULL keys                  |
| `JOIN ... ON a.key <=> b.key` | Can match NULL keys                       |

### :material-animation-play: Interactive Visualization — Filter Outcomes

<div id="viz-null-filter" class="ts-viz"></div>

Select a predicate to see which rows Spark 4.2 keeps and which rows are dropped because the condition evaluates to `NULL`.

______________________________________________________________________

## :material-magnify: Verified Behavior

### `WHERE` drops NULL results

```sql
SELECT name
FROM
VALUES
    ('Joe', 30),
    ('Marry', CAST(NULL AS INT)),
    ('Mike', 18),
    ('Fred', 50),
    ('Albert', CAST(NULL AS INT)),
    ('Michelle', 30),
    ('Dan', 50)
AS person(name, age)
WHERE age > 0
ORDER BY name;
```

```text
+--------+
|name    |
+--------+
|Dan     |
|Fred    |
|Joe     |
|Michelle|
|Mike    |
+--------+
```

### Include NULL rows explicitly

```sql
SELECT name
FROM
VALUES
    ('Joe', 30),
    ('Marry', CAST(NULL AS INT)),
    ('Mike', 18),
    ('Fred', 50),
    ('Albert', CAST(NULL AS INT)),
    ('Michelle', 30),
    ('Dan', 50)
AS person(name, age)
WHERE age > 0 OR age IS NULL
ORDER BY name;
```

```text
+--------+
|name    |
+--------+
|Albert  |
|Dan     |
|Fred    |
|Joe     |
|Marry   |
|Michelle|
|Mike    |
+--------+
```

### `HAVING` evaluates after NULLs have already been grouped

```sql
SELECT age, COUNT(*) AS cnt, MAX(age) AS mx
FROM
VALUES
    (30),
    (CAST(NULL AS INT)),
    (18),
    (50),
    (CAST(NULL AS INT)),
    (30),
    (50)
AS t(age)
GROUP BY age
HAVING MAX(age) > 18
ORDER BY age;
```

```text
+---+---+---+
|age|cnt|mx |
+---+---+---+
|30 |2  |30 |
|50 |2  |50 |
+---+---+---+
```

### Standard joins do not match NULL keys

```sql
SELECT a.id AS left_id, b.id AS right_id
FROM
VALUES
    (1, 10),
    (2, CAST(NULL AS INT)),
    (3, 20),
    (4, CAST(NULL AS INT))
AS a(id, k)
JOIN
VALUES
    (10, 10),
    (20, CAST(NULL AS INT)),
    (30, 20),
    (40, CAST(NULL AS INT))
AS b(id, k)
ON a.k = b.k
ORDER BY left_id, right_id;
```

```text
+-------+--------+
|left_id|right_id|
+-------+--------+
|1      |10      |
|3      |30      |
+-------+--------+
```

### Null-safe joins can match NULL keys

```sql
SELECT a.id AS left_id, b.id AS right_id
FROM
VALUES
    (1, 10),
    (2, CAST(NULL AS INT)),
    (3, 20),
    (4, CAST(NULL AS INT))
AS a(id, k)
JOIN
VALUES
    (10, 10),
    (20, CAST(NULL AS INT)),
    (30, 20),
    (40, CAST(NULL AS INT))
AS b(id, k)
ON a.k <=> b.k
ORDER BY left_id, right_id;
```

```text
+-------+--------+
|left_id|right_id|
+-------+--------+
|1      |10      |
|2      |20      |
|2      |40      |
|3      |30      |
|4      |20      |
|4      |40      |
+-------+--------+
```

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- `WHERE` and `HAVING` keep `TRUE`, not `TRUE or NULL`.
- Add `OR col IS NULL` only when unknown values should survive the filter.
- Use `<=>` when NULL join keys should match.

<script src="../../assets/js/querying-nulls-viz.js"></script>
