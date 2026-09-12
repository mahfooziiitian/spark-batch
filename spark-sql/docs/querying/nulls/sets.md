# :material-null: NULL in Set Operations

Spark 4.2 uses null-aware row comparison for set operations, so NULL rows can match across `UNION`, `INTERSECT`, and `EXCEPT`.

______________________________________________________________________

## :material-sitemap: Overview

| Operator    | Verified NULL behavior                                                    |
| ----------- | ------------------------------------------------------------------------- |
| `UNION`     | Deduplicates identical rows, including identical NULL rows                |
| `UNION ALL` | Preserves every row, including duplicate NULL rows                        |
| `INTERSECT` | Keeps a NULL row if it appears on both sides                              |
| `EXCEPT`    | Removes a NULL row from the left if the same NULL row exists on the right |

### :material-animation-play: Interactive Visualization — Set Operations with NULLs

<div id="viz-null-sets" class="ts-viz"></div>

Switch between set operators to see how Spark 4.2 compares rows that contain NULL. Matching NULL rows are treated as the same row for set membership.

______________________________________________________________________

## :material-flask-outline: Verified Examples

### `UNION`

```sql
SELECT c
FROM
VALUES
    (1),
    (CAST(NULL AS INT)),
    (CAST(NULL AS INT))
AS a(c)
UNION
SELECT c
FROM
VALUES
    (CAST(NULL AS INT)),
    (2)
AS b(c)
ORDER BY c;
```

```text
+----+
|c   |
+----+
|NULL|
|1   |
|2   |
+----+
```

### `UNION ALL`

```sql
SELECT c
FROM
VALUES
    (1),
    (CAST(NULL AS INT)),
    (CAST(NULL AS INT))
AS a(c)
UNION ALL
SELECT c
FROM
VALUES
    (CAST(NULL AS INT)),
    (2)
AS b(c)
ORDER BY c;
```

```text
+----+
|c   |
+----+
|NULL|
|NULL|
|NULL|
|1   |
|2   |
+----+
```

### `INTERSECT`

```sql
SELECT c
FROM
VALUES
    (1),
    (CAST(NULL AS INT)),
    (2)
AS a(c)
INTERSECT
SELECT c
FROM
VALUES
    (CAST(NULL AS INT)),
    (2),
    (3)
AS b(c)
ORDER BY c;
```

```text
+----+
|c   |
+----+
|NULL|
|2   |
+----+
```

### `EXCEPT`

```sql
SELECT c
FROM
VALUES
    (1),
    (CAST(NULL AS INT)),
    (2)
AS a(c)
EXCEPT
SELECT c
FROM
VALUES
    (CAST(NULL AS INT)),
    (3)
AS b(c)
ORDER BY c;
```

```text
+---+
|c  |
+---+
|1  |
|2  |
+---+
```

### Full-row comparison still matters

A row like `(NULL, 'x')` is considered the same as another `(NULL, 'x')`, but not the same as `(NULL, 'y')`. Spark compares the whole row, not just the NULL column.

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- `UNION` and `INTERSECT` can keep a single NULL row because Spark compares rows null-safely.
- `UNION ALL` never deduplicates.
- `EXCEPT` removes left-side NULL rows when the same NULL row exists on the right.

<script src="../../assets/js/querying-nulls-viz.js"></script>
