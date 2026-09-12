# :material-null: NULL in Aggregate Functions

Spark 4.2 aggregate functions ignore NULL inputs unless the aggregate is explicitly counting rows with `COUNT(*)`.

______________________________________________________________________

## :material-sitemap: Overview

| Aggregate                  | Verified NULL behavior                                   |
| -------------------------- | -------------------------------------------------------- |
| `COUNT(*)`                 | Counts every row                                         |
| `COUNT(col)`               | Counts only non-NULL values                              |
| `SUM`, `AVG`, `MIN`, `MAX` | Skip NULL inputs                                         |
| `EVERY`, `ANY`, `SOME`     | Skip non-values; all-NULL and empty inputs return `NULL` |

### :material-animation-play: Interactive Visualization — Aggregate Inputs vs Results

<div id="viz-null-aggregate" class="ts-viz"></div>

Toggle a Spark aggregate to see which rows participate. The highlighted values are the inputs Spark 4.2 actually uses for the result.

______________________________________________________________________

## :material-table: Verified Results

Using ages `30, NULL, 18, 50, NULL, 30, 50`:

| Query        | Result |
| ------------ | ------ |
| `COUNT(*)`   | `7`    |
| `COUNT(age)` | `5`    |
| `SUM(age)`   | `178`  |
| `AVG(age)`   | `35.6` |
| `MAX(age)`   | `50`   |
| `MIN(age)`   | `18`   |

On all-NULL input, `COUNT(col)` returns `0`, `COUNT(*)` still counts rows, and `SUM` / `AVG` / `MIN` / `MAX` return `NULL`.

______________________________________________________________________

## :material-flask-outline: Verified Examples

### `COUNT(*)` vs `COUNT(col)`

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(age) AS known_ages
FROM
VALUES
    (100, 'Joe', 30),
    (200, 'Marry', CAST(NULL AS INT)),
    (300, 'Mike', 18),
    (400, 'Fred', 50),
    (500, 'Albert', CAST(NULL AS INT)),
    (600, 'Michelle', 30),
    (700, 'Dan', 50)
AS person(id, name, age);
```

```text
+----------+----------+
|total_rows|known_ages|
+----------+----------+
|7         |5         |
+----------+----------+
```

### `SUM`, `AVG`, `MAX`, and `MIN` skip NULLs

```sql
SELECT
    SUM(age) AS age_sum,
    AVG(age) AS age_avg,
    MAX(age) AS age_max,
    MIN(age) AS age_min
FROM
VALUES
    (30),
    (CAST(NULL AS INT)),
    (18),
    (50),
    (CAST(NULL AS INT)),
    (30),
    (50)
AS t(age);
```

```text
+-------+-------+-------+-------+
|age_sum|age_avg|age_max|age_min|
+-------+-------+-------+-------+
|178    |35.6   |50     |18     |
+-------+-------+-------+-------+
```

### Empty input vs all-NULL input

```sql
SELECT
    COUNT(*) AS cnt_all,
    COUNT(v) AS cnt_v,
    SUM(v) AS sum_v,
    AVG(v) AS avg_v,
    MAX(v) AS max_v,
    MIN(v) AS min_v
FROM
VALUES
    (CAST(NULL AS INT)),
    (CAST(NULL AS INT))
AS t(v);
```

```text
+-------+-----+-----+-----+-----+-----+
|cnt_all|cnt_v|sum_v|avg_v|max_v|min_v|
+-------+-----+-----+-----+-----+-----+
|2      |0    |NULL |NULL |NULL |NULL |
+-------+-----+-----+-----+-----+-----+
```

```sql
SELECT
    COUNT(*) AS cnt_all,
    COUNT(v) AS cnt_v,
    SUM(v) AS sum_v,
    AVG(v) AS avg_v,
    MAX(v) AS max_v,
    MIN(v) AS min_v
FROM
VALUES
    (1)
AS t(v)
WHERE 1 = 0;
```

```text
+-------+-----+-----+-----+-----+-----+
|cnt_all|cnt_v|sum_v|avg_v|max_v|min_v|
+-------+-----+-----+-----+-----+-----+
|0      |0    |NULL |NULL |NULL |NULL |
+-------+-----+-----+-----+-----+-----+
```

### `GROUP BY` still creates one NULL group

```sql
SELECT age, COUNT(*) AS cnt
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
+----+---+
|age |cnt|
+----+---+
|NULL|2  |
|18  |1  |
|30  |2  |
|50  |2  |
+----+---+
```

### Boolean aggregates with only NULL inputs

```sql
SELECT
    EVERY(v) AS every_v,
    ANY(v) AS any_v,
    SOME(v) AS some_v
FROM
VALUES
    (CAST(NULL AS BOOLEAN)),
    (CAST(NULL AS BOOLEAN))
AS t(v);
```

```text
+-------+-----+------+
|every_v|any_v|some_v|
+-------+-----+------+
|NULL   |NULL |NULL  |
+-------+-----+------+
```

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- Use `COUNT(*)` when you need row counts.
- Use `COUNT(col)` when you need counts of known values.
- Expect arithmetic aggregates on empty or all-NULL inputs to return `NULL`.

<script src="../../assets/js/querying-nulls-viz.js"></script>
