# :material-code-array: Array Higher-Order Functions

Array HOFs accept a lambda and produce a new array, a boolean, or a scalar
by applying the lambda to each element of the input array.

______________________________________________________________________

## :material-table: Quick Reference

| HOF                               | Lambda signature | Returns | Description                          |
| --------------------------------- | ---------------- | ------- | ------------------------------------ |
| `TRANSFORM(arr, x -> expr)`       | `x -> expr`      | Array   | Map each element to a new value      |
| `TRANSFORM(arr, (x,i) -> expr)`   | `(x, i) -> expr` | Array   | Map with element index               |
| `FILTER(arr, x -> bool)`          | `x -> bool`      | Array   | Keep elements where lambda is true   |
| `FILTER(arr, (x,i) -> bool)`      | `(x, i) -> bool` | Array   | Filter with element index            |
| `EXISTS(arr, x -> bool)`          | `x -> bool`      | Boolean | True if any element satisfies lambda |
| `FORALL(arr, x -> bool)`          | `x -> bool`      | Boolean | True if all elements satisfy lambda  |
| `ZIP_WITH(a1, a2, (x,y) -> expr)` | `(x, y) -> expr` | Array   | Merge two arrays element-wise        |

### :material-animation-play: Interactive Visualization

<div id="viz-zip-with-pad" class="ts-viz"></div>

`ZIP_WITH` always produces an array as long as the **longer** input, padding the
shorter array with `NULL` — it never truncates. Click a lambda call to see the
`NULL` propagate through `(x, y) -> expr` when one side is missing.

______________________________________________________________________

## :material-function-variant: TRANSFORM

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

______________________________________________________________________

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

______________________________________________________________________

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

______________________________________________________________________

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

______________________________________________________________________

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

-- Build key-value structs from separate key/value arrays
SELECT ZIP_WITH(
    ARRAY('env', 'version', 'owner'),
    ARRAY('prod', '1.2', 'teamA'),
    (k, v) -> NAMED_STRUCT('key', k, 'value', v)
) AS config;
-- [{key: env, value: prod}, {key: version, value: 1.2}, {key: owner, value: teamA}]

-- Conditional merge: take the max of each position
SELECT
    ZIP_WITH(forecast, actuals, (f, a) -> GREATEST(f, a)) AS upper_bound
FROM projections;

-- Unequal lengths: shorter array is NULL-padded to the longer length (never truncated)
SELECT ZIP_WITH(ARRAY(1, 2, 3), ARRAY(10, 20, 30, 40, 50), (a, b) -> a + b) AS padded;
-- [11, 22, 33, NULL, NULL]  <- a is NULL for indices 3, 4, so a + b is NULL

-- Guard the padding with COALESCE so a missing side doesn't poison the sum
SELECT ZIP_WITH(ARRAY(1, 2, 3), ARRAY(10, 20, 30, 40, 50),
    (a, b) -> COALESCE(a, 0) + COALESCE(b, 0)
) AS padded_safe;
-- [11, 22, 33, 40, 50]
```

______________________________________________________________________

## :material-alert-circle-outline: Three-Valued Logic Gotchas

`FILTER`, `EXISTS`, and `FORALL` all take a **boolean-returning** lambda, but SQL
boolean logic has three states: `TRUE`, `FALSE`, and `NULL` (unknown). A lambda
that evaluates to `NULL` — typically from comparing against a `NULL` field —
is *not* the same as `FALSE`:

| Function | When the lambda is `NULL` for an element/all elements              | Not to be confused with                             |
| -------- | ------------------------------------------------------------------ | --------------------------------------------------- |
| `FILTER` | That element is **excluded**, same as `FALSE`                      | An explicit "kept as unknown" state                 |
| `EXISTS` | Returns `NULL` if no element is `TRUE` but at least one is `NULL`  | Returning `FALSE` when nothing definitively matched |
| `FORALL` | Returns `NULL` if no element is `FALSE` but at least one is `NULL` | Returning `TRUE` when nothing definitively failed   |

```sql
-- FILTER: Bob's NULL score makes the predicate NULL, so he is silently dropped
SELECT FILTER(
    ARRAY(NAMED_STRUCT('name', 'Alice', 'score', 90),
          NAMED_STRUCT('name', 'Bob', 'score', CAST(NULL AS INT))),
    s -> s.score > 80
) AS filtered;
-- [{name: Alice, score: 90}]  -- not an error, but Bob vanished without a FALSE

-- EXISTS / FORALL: NULL, not FALSE/TRUE, when a comparison is unknown
SELECT EXISTS(ARRAY(NAMED_STRUCT('age', 20), NAMED_STRUCT('age', CAST(NULL AS INT))),
              s -> s.age > 30) AS any_over_30;
-- NULL  -- no element is TRUE, but the NULL age is "unknown", not "definitely under 30"

-- Fix: make the NULL case explicit with COALESCE so the result is always TRUE/FALSE
SELECT EXISTS(ARRAY(NAMED_STRUCT('age', 20), NAMED_STRUCT('age', CAST(NULL AS INT))),
              s -> COALESCE(s.age > 30, FALSE)) AS any_over_30_definite;
-- false
```

`TRANSFORM` has a related but different gotcha: it does **not** exclude `NULL`
elements — it still calls the lambda on them, so an unguarded expression
propagates the `NULL` straight through to the output:

```sql
SELECT TRANSFORM(ARRAY(1, NULL, 3), x -> x * 2) AS doubled;
-- [2, NULL, 6]   <- the NULL element produces a NULL output element, not an error or skip
```

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                                              | Behaviour                                                           | Fix                                             |
| ---------------------------------------------------- | ------------------------------------------------------------------- | ----------------------------------------------- |
| `FILTER(arr, x -> x > 5)` used as boolean in `WHERE` | Returns array, not bool                                             | Wrap: `WHERE size(FILTER(arr, x -> x > 5)) > 0` |
| `ZIP_WITH` on arrays of different length             | **Pads with `NULL`** to the longer array's length (never truncates) | Guard lambda body with `COALESCE(x, default)`   |
| Lambda parameter shadowing outer column              | Silent wrong results                                                | Use distinct names                              |
| Calling a UDF inside a lambda                        | Analysis error                                                      | Rewrite as SQL expression                       |
