# :material-puzzle: Lambda Patterns

Reusable patterns that combine HOFs and lambdas for real-world data engineering tasks.

---

## :material-pipe: Filter → Transform Pipeline

Apply a filter first to narrow the array, then transform only the matching elements.

```sql
-- Keep even numbers, then square them
SELECT TRANSFORM(
    FILTER(ARRAY(1, 2, 3, 4, 5, 6), x -> x % 2 = 0),
    x -> x * x
) AS even_squares;
-- Result: [4, 16, 36]

-- Keep error tags, then uppercase them
SELECT
    event_id,
    TRANSFORM(
        FILTER(tags, t -> t LIKE 'error%'),
        t -> UPPER(t)
    ) AS error_tags_upper
FROM events;
```

---

## :material-check-all: Filter → EXISTS / FORALL Guards

Validate an array after filtering to drive row-level decisions.

```sql
-- Rows where ALL remaining scores (after dropping nulls) are passing
SELECT student_id
FROM results
WHERE FORALL(
    FILTER(scores, s -> s IS NOT NULL),
    s -> s >= 50
);

-- Rows where ANY critical tag exists after trimming whitespace
SELECT event_id
FROM events
WHERE EXISTS(
    TRANSFORM(tags, t -> TRIM(LOWER(t))),
    t -> t IN ('critical', 'alert', 'p0')
);
```

---

## :material-counter: AGGREGATE + TRANSFORM: Normalise an Array

```sql
-- Normalise scores to 0–1 range using aggregate to find max, then transform
SELECT
    student_id,
    TRANSFORM(
        scores,
        s -> ROUND(s / AGGREGATE(scores, 0D, (acc, x) -> IF(x > acc, x, acc)), 3)
    ) AS normalised
FROM results;
```

---

## :material-compare-horizontal: ZIP_WITH + AGGREGATE: Dot Product

```sql
-- Dot product of two equal-length vectors
SELECT AGGREGATE(
    ZIP_WITH(ARRAY(1.0, 2.0, 3.0), ARRAY(4.0, 5.0, 6.0), (a, b) -> a * b),
    0D,
    (acc, x) -> acc + x
) AS dot_product;
-- Result: 32.0  (1×4 + 2×5 + 3×6)
```

---

## :material-layers: Nested Lambda: Transform Array of Maps

```sql
-- For each row's attribute map array, keep only 'priority' and 'region' keys
SELECT
    order_id,
    TRANSFORM(
        attribute_snapshots,
        snap -> MAP_FILTER(snap, (k, v) -> k IN ('priority', 'region'))
    ) AS trimmed_snapshots
FROM order_history;
```

---

## :material-sort: Sort + Slice: Top-N Elements

```sql
-- Top 3 scores per student (sort descending, slice first 3)
SELECT
    student_id,
    SLICE(SORT_ARRAY(scores, false), 1, 3) AS top3_scores
FROM results;

-- Top-3 with FILTER guard (ignore nulls)
SELECT
    student_id,
    SLICE(
        SORT_ARRAY(FILTER(scores, s -> s IS NOT NULL), false),
        1, 3
    ) AS top3_valid
FROM results;
```

---

## :material-tag-multiple: Struct Array Processing

```sql
-- Extract names of team members with score > 80
CREATE OR REPLACE TEMP VIEW teams AS
SELECT * FROM VALUES
  (1, ARRAY(
        NAMED_STRUCT('name', 'Alice', 'score', 92),
        NAMED_STRUCT('name', 'Bob',   'score', 75),
        NAMED_STRUCT('name', 'Carol', 'score', 88)
     )),
  (2, ARRAY(
        NAMED_STRUCT('name', 'Dave',  'score', 65),
        NAMED_STRUCT('name', 'Eve',   'score', 91)
     ))
AS t(team_id, members);

-- Names of high scorers
SELECT
    team_id,
    TRANSFORM(
        FILTER(members, m -> m.score > 80),
        m -> m.name
    ) AS star_performers
FROM teams;
-- team_id | star_performers
-- --------|----------------
-- 1       | [Alice, Carol]
-- 2       | [Eve]

-- Aggregate: team average score
SELECT
    team_id,
    AGGREGATE(
        members,
        NAMED_STRUCT('total', 0D, 'cnt', 0),
        (acc, m) -> NAMED_STRUCT('total', acc.total + m.score, 'cnt', acc.cnt + 1),
        acc -> ROUND(acc.total / acc.cnt, 1)
    ) AS avg_score
FROM teams;
```

---

## :material-numeric: Index-Based Operations

```sql
-- Tag each element with its 1-based position
SELECT TRANSFORM(ARRAY('a', 'b', 'c', 'd'),
    (v, i) -> NAMED_STRUCT('pos', i + 1, 'val', v)
) AS indexed;
-- [{pos:1, val:a}, {pos:2, val:b}, ...]

-- Keep only first and last element using index and SIZE
SELECT
    id,
    FILTER(items, (v, i) -> i = 0 OR i = SIZE(items) - 1) AS first_last
FROM lists;
```

---

## :material-speedometer: Performance Tips

| Tip | Reason |
|-----|--------|
| Pre-filter rows with `array_contains` before HOFs | `array_contains` can be pushed to file scans; HOFs cannot |
| Avoid deeply nested lambdas (3+ levels) | Hard to read; consider LATERAL VIEW + inline instead |
| Use `ARRAY_MIN` / `ARRAY_MAX` / `ARRAY_JOIN` over `AGGREGATE` equivalents | Native functions are faster |
| Cache the result of an expensive HOF in a CTE | Avoids re-computing the same HOF multiple times |
| Prefer `SIZE(FILTER(...)) > 0` over `EXISTS(...)` for complex predicates | Both are equivalent; `EXISTS` is slightly cleaner |

---

## :material-alert-circle: Common Mistakes

| Mistake | Result | Fix |
|---------|--------|-----|
| Using HOF result directly in `WHERE` | Type mismatch (array, not bool) | Wrap with `SIZE(...) > 0` or `EXISTS` |
| Lambda parameter name matches outer column | Silent shadowing | Use unique parameter names |
| `ZIP_WITH` on unequal-length arrays | Truncated to shorter array | Pad arrays or guard with `SIZE` check |
| `AGGREGATE` on NULL array | Returns NULL | Guard: `COALESCE(AGGREGATE(...), default)` |
| Calling a registered UDF inside a lambda | Analysis error | Rewrite as SQL expression or use `LATERAL VIEW` |
