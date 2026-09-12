# :material-null: NULL Ordering

Spark 4.2 sorts NULLs first for ascending order and last for descending order unless you override the placement explicitly.

______________________________________________________________________

## :material-sitemap: Overview

| Query shape                | Verified default  |
| -------------------------- | ----------------- |
| `ORDER BY col ASC`         | `NULLS FIRST`     |
| `ORDER BY col DESC`        | `NULLS LAST`      |
| `ORDER BY ... NULLS FIRST` | Explicit override |
| `ORDER BY ... NULLS LAST`  | Explicit override |

### :material-animation-play: Interactive Visualization — NULL Sort Placement

<div id="viz-null-ordering" class="ts-viz"></div>

Toggle direction and NULL placement to see how the sorted row order changes. This matches the Spark 4.2 defaults and overrides shown below.

______________________________________________________________________

## :material-code-tags: Syntax

```sql
ORDER BY col [ASC | DESC] [NULLS FIRST | NULLS LAST]
```

______________________________________________________________________

## :material-flask-outline: Verified Examples

### Ascending default

```sql
SELECT age
FROM
VALUES
    (30),
    (CAST(NULL AS INT)),
    (18),
    (50),
    (CAST(NULL AS INT))
AS t(age)
ORDER BY age ASC;
```

```text
+----+
|age |
+----+
|NULL|
|NULL|
|18  |
|30  |
|50  |
+----+
```

### Ascending with `NULLS LAST`

```sql
SELECT age
FROM
VALUES
    (30),
    (CAST(NULL AS INT)),
    (18),
    (50),
    (CAST(NULL AS INT))
AS t(age)
ORDER BY age ASC NULLS LAST;
```

```text
+----+
|age |
+----+
|18  |
|30  |
|50  |
|NULL|
|NULL|
+----+
```

### Descending default

```sql
SELECT age
FROM
VALUES
    (30),
    (CAST(NULL AS INT)),
    (18),
    (50),
    (CAST(NULL AS INT))
AS t(age)
ORDER BY age DESC;
```

```text
+----+
|age |
+----+
|50  |
|30  |
|18  |
|NULL|
|NULL|
+----+
```

### Descending with `NULLS FIRST`

```sql
SELECT age
FROM
VALUES
    (30),
    (CAST(NULL AS INT)),
    (18),
    (50),
    (CAST(NULL AS INT))
AS t(age)
ORDER BY age DESC NULLS FIRST;
```

```text
+----+
|age |
+----+
|NULL|
|NULL|
|50  |
|30  |
|18  |
+----+
```

### Window ordering accepts the same NULL directives

```sql
SELECT age, ROW_NUMBER() OVER (ORDER BY age DESC NULLS FIRST) AS rn
FROM
VALUES
    (30),
    (CAST(NULL AS INT)),
    (18),
    (50),
    (CAST(NULL AS INT))
AS t(age);
```

```text
+----+---+
|age |rn |
+----+---+
|NULL|1  |
|NULL|2  |
|50  |3  |
|30  |4  |
|18  |5  |
+----+---+
```

______________________________________________________________________

## :material-lightbulb-outline: Practical Takeaways

- Remember the defaults: ascending puts NULLs first, descending puts NULLs last.
- Add `NULLS FIRST` or `NULLS LAST` when output order must be obvious to readers.
- The same syntax works in window `ORDER BY` clauses.

<script src="../../assets/js/querying-nulls-viz.js"></script>
