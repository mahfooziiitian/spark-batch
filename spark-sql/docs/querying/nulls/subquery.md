# :material-null: NULL in Subqueries

NULL values inside subqueries affect `EXISTS`, `IN`, and `NOT IN` very differently in Spark 4.2.

______________________________________________________________________

## :material-sitemap: Overview

| Predicate                | Verified effect of NULLs in subquery output                                                                        |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `EXISTS (subquery)`      | Only row existence matters                                                                                         |
| `NOT EXISTS (subquery)`  | Only row absence matters                                                                                           |
| `expr IN (subquery)`     | Returns `NULL` when no match is found and the subquery also contains NULL                                          |
| `expr NOT IN (subquery)` | Matching rows return `FALSE`; remaining rows return `NULL`, so `WHERE` keeps nothing if the subquery contains NULL |

### :material-animation-play: Interactive Visualization — `IN`, `NOT IN`, and `NOT EXISTS`

<div id="viz-null-subquery" class="ts-viz"></div>

Pick an outer value and predicate to see how a subquery containing `50` and `NULL` changes the result. This makes the `NOT IN` trap visible row by row.

______________________________________________________________________

## :material-magnify: Verified Behavior

### `EXISTS` and `NOT EXISTS` ignore subquery column values

```sql
SELECT
    EXISTS (SELECT NULL) AS exists_null,
    NOT EXISTS (SELECT NULL) AS not_exists_null,
    NOT EXISTS (SELECT 1 WHERE 1 = 0) AS not_exists_empty;
```

```text
+-----------+---------------+----------------+
|exists_null|not_exists_null|not_exists_empty|
+-----------+---------------+----------------+
|true       |false          |true            |
+-----------+---------------+----------------+
```

### `IN` with a NULL in the subquery result

```sql
SELECT name, age
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
WHERE age IN (
    SELECT age
    FROM
    VALUES
        (50),
        (CAST(NULL AS INT))
    AS t(age)
)
ORDER BY name;
```

```text
+----+---+
|name|age|
+----+---+
|Dan |50 |
|Fred|50 |
+----+---+
```

Rows with age `30` or `18` do not become `FALSE`; they become `NULL`, so `WHERE` excludes them.

### `NOT IN` with a NULL in the subquery result

```sql
SELECT name, age
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
WHERE age NOT IN (
    SELECT age
    FROM
    VALUES
        (50),
        (CAST(NULL AS INT))
    AS t(age)
)
ORDER BY name;
```

```text
+----+---+
|name|age|
+----+---+
+----+---+
```

The `50` rows evaluate to `FALSE`; the remaining rows evaluate to `NULL`. Because `WHERE` keeps only `TRUE`, no rows survive.

### `NOT EXISTS` avoids the trap

```sql
SELECT p.name, p.age
FROM
VALUES
    ('Joe', 30),
    ('Marry', CAST(NULL AS INT)),
    ('Mike', 18),
    ('Fred', 50),
    ('Albert', CAST(NULL AS INT)),
    ('Michelle', 30),
    ('Dan', 50)
AS p(name, age)
WHERE NOT EXISTS (
    SELECT 1
    FROM
    VALUES
        (50),
        (CAST(NULL AS INT))
    AS t(age)
    WHERE t.age = p.age
)
ORDER BY p.name;
```

```text
+--------+----+
|name    |age |
+--------+----+
|Albert  |NULL|
|Joe     |30  |
|Marry   |NULL|
|Michelle|30  |
|Mike    |18  |
+--------+----+
```

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- Prefer `EXISTS` and `NOT EXISTS` for membership checks.
- Treat `NOT IN` as unsafe unless the subquery explicitly filters out NULLs.
- Remember that `IN` can return `NULL`, not just `TRUE` or `FALSE`.

<script src="../../assets/js/querying-nulls-viz.js"></script>
