# :material-code-array: Array Higher-Order Functions

Array HOFs accept a lambda and produce a new array, a boolean, or a scalar
by applying the lambda to each element of the input array.

---

## :material-table: Quick Reference

| HOF | Lambda signature | Returns | Description |
|-----|-----------------|---------|-------------|
| `TRANSFORM(arr, x -> expr)` | `x -> expr` | Array | Map each element to a new value |
| `TRANSFORM(arr, (x,i) -> expr)` | `(x, i) -> expr` | Array | Map with element index |
| `FILTER(arr, x -> bool)` | `x -> bool` | Array | Keep elements where lambda is true |
| `FILTER(arr, (x,i) -> bool)` | `(x, i) -> bool` | Array | Filter with element index |
| `EXISTS(arr, x -> bool)` | `x -> bool` | Boolean | True if any element satisfies lambda |
| `FORALL(arr, x -> bool)` | `x -> bool` | Boolean | True if all elements satisfy lambda |
| `ZIP_WITH(a1, a2, (x,y) -> expr)` | `(x, y) -> expr` | Array | Merge two arrays element-wise |

---

## :material-transform: TRANSFORM

```sql
-- Arithmetic
SELECT TRANSFORM(ARRAY(1, 2, 3, 4, 5), x -> x * x) AS squares;
-- [1, 4, 9, 16, 25]

-- String operation
SELECT TRANSFORM(ARRAY('hello', 'world'), x -> UPPER(x)) AS upper;
-- ['HELLO', 'WORLD']

-- Conditional
SELECT TRANSFORM(ARRAY(10, -5, 8, -3, 0),
    x -> CASE WHEN x < 0 THEN 0 ELSE x END
) AS clipped;
-- [10, 0, 8, 0, 0]

-- With index: add 1-based rank prefix
SELECT TRANSFORM(ARRAY('gold', 'silver', 'bronze'),
    (medal, pos) -> CONCAT(CAST(pos + 1 AS STRING), '. ', medal)
) AS ranked;
-- ['1. gold', '2. silver', '3. bronze']

-- Real table: normalise scores per row
SELECT
    student_id,
    TRANSFORM(scores, s -> ROUND(s * 100.0 / ARRAY_MAX(scores), 1)) AS pct_scores
FROM student_results;
```

---

## :material-filter: FILTER

```sql
-- Keep elements above threshold
SELECT FILTER(ARRAY(10, 55, 30, 80, 15), x -> x > 50) AS high_scores;
-- [55, 80]

-- Keep strings matching a pattern
SELECT FILTER(ARRAY('error_login', 'info_page', 'error_db', 'warn_slow'),
    x -> x LIKE 'error%'
) AS errors;
-- ['error_login', 'error_db']

-- Filter with index: keep even-index elements (0, 2, 4, ...)
SELECT FILTER(ARRAY('a', 'b', 'c', 'd', 'e'), (x, i) -> i % 2 = 0) AS even_pos;
-- ['a', 'c', 'e']

-- Filter struct array: only active users
SELECT
    account_id,
    FILTER(team_members, m -> m.active = true) AS active_members
FROM teams;

-- Filter + size as a row predicate
SELECT event_id
FROM events
WHERE size(FILTER(tags, t -> t LIKE 'critical%')) > 0;
```

---

## :material-check-circle: EXISTS

```sql
-- Any score above 90?
SELECT EXISTS(ARRAY(72, 88, 95, 61), x -> x > 90) AS has_top_score;
-- true

-- Check if any tag starts with 'error'
SELECT event_id
FROM events
WHERE EXISTS(tags, t -> t LIKE 'error%');

-- NULL-safe: does any element equal NULL?
SELECT EXISTS(ARRAY(1, NULL, 3), x -> x IS NULL) AS has_null;
-- true
```

---

## :material-check-all: FORALL

```sql
-- Are all scores passing (>= 50)?
SELECT FORALL(ARRAY(72, 88, 55, 91), x -> x >= 50) AS all_passing;
-- true

-- Are all prices positive?
SELECT order_id
FROM orders
WHERE FORALL(line_item_prices, p -> p > 0);

-- Vacuous truth: FORALL on empty array always returns true
SELECT FORALL(ARRAY(), x -> x > 100);
-- true
```

---

## :material-zip-disk: ZIP_WITH

```sql
-- Element-wise sum of two arrays
SELECT ZIP_WITH(ARRAY(1, 2, 3), ARRAY(10, 20, 30), (a, b) -> a + b) AS sums;
-- [11, 22, 33]

-- Concatenate labels with values
SELECT ZIP_WITH(
    ARRAY('min', 'avg', 'max'),
    ARRAY(10, 45, 90),
    (label, val) -> CONCAT(label, '=', CAST(val AS STRING))
) AS stats;
-- ['min=10', 'avg=45', 'max=90']

-- Real table: compute delta between two score snapshots
SELECT
    student_id,
    ZIP_WITH(scores_before, scores_after, (b, a) -> a - b) AS improvements
FROM score_snapshots;

-- Conditional merge: take the max of each position
SELECT
    ZIP_WITH(forecast, actuals, (f, a) -> GREATEST(f, a)) AS upper_bound
FROM projections;
```

---

## :material-alert-circle: Common Mistakes

| Mistake | Behaviour | Fix |
|---------|-----------|-----|
| `FILTER(arr, x -> x > 5)` used as boolean in `WHERE` | Returns array, not bool | Wrap: `WHERE size(FILTER(arr, x -> x > 5)) > 0` |
| `ZIP_WITH` on arrays of different length | Returns array of shorter length | Pad arrays to equal length first |
| Lambda parameter shadowing outer column | Silent wrong results | Use distinct names |
| Calling a UDF inside a lambda | Analysis error | Rewrite as SQL expression |
