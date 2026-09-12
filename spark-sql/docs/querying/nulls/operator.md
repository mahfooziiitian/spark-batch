# :material-null: NULL in GROUP BY, DISTINCT, and PARTITION BY

Spark 4.2 treats NULLs as one structural value for grouping, deduplication, and partitioning, even though `NULL = NULL` is not `TRUE` in ordinary comparisons.

______________________________________________________________________

## :material-sitemap: Overview

| Operation             | Verified NULL behavior                                  |
| --------------------- | ------------------------------------------------------- |
| `GROUP BY col`        | All NULL rows land in one group                         |
| `SELECT DISTINCT col` | Output contains at most one NULL per distinct row shape |
| `PARTITION BY col`    | All NULL rows share one partition key                   |

### :material-animation-play: Interactive Visualization — Grouping and Deduplication

<div id="viz-null-operator" class="ts-viz"></div>

Switch between grouping and deduplication to see how Spark 4.2 buckets NULL values. The visualization uses the same sample rows as the examples below.

______________________________________________________________________

## :material-flask-outline: Verified Examples

### `GROUP BY` creates one NULL bucket

```sql
SELECT age, COUNT(*) AS cnt, COUNT(age) AS non_null_age
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
ORDER BY age;
```

```text
+----+---+------------+
|age |cnt|non_null_age|
+----+---+------------+
|NULL|2  |0           |
|18  |1  |1           |
|30  |2  |2           |
|50  |2  |2           |
+----+---+------------+
```

### `DISTINCT` keeps one NULL row per distinct row shape

```sql
SELECT DISTINCT age
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
ORDER BY age;
```

```text
+----+
|age |
+----+
|NULL|
|18  |
|30  |
|50  |
+----+
```

### Multi-column `DISTINCT` still compares full rows

```sql
SELECT DISTINCT a, b
FROM
VALUES
    (CAST(NULL AS INT), 'x'),
    (CAST(NULL AS INT), 'x'),
    (CAST(NULL AS INT), 'y')
AS t(a, b)
ORDER BY b;
```

```text
+----+---+
|a   |b  |
+----+---+
|NULL|x  |
|NULL|y  |
+----+---+
```

### Window partitioning groups NULL keys together

```sql
SELECT age, COUNT(*) OVER (PARTITION BY age) AS cnt
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
ORDER BY age, cnt;
```

```text
+----+---+
|age |cnt|
+----+---+
|NULL|2  |
|NULL|2  |
|18  |1  |
|30  |2  |
|30  |2  |
|50  |2  |
|50  |2  |
+----+---+
```

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- `GROUP BY` and `DISTINCT` use structural equality, not normal comparison results.
- One NULL column value can still appear in multiple distinct rows when other columns differ.
- `COUNT(age)` inside a NULL group is `0` because the values are still NULL.

<script src="../../assets/js/querying-nulls-viz.js"></script>
