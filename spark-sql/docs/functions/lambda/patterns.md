# :material-puzzle: Lambda Patterns

Reusable patterns that combine HOFs and lambdas for real-world data engineering tasks.

______________________________________________________________________

## :material-pipe: Filter → Transform Pipeline

### :material-animation-play: Interactive Visualization

<div id="viz-pipeline" class="ts-viz"></div>

Watch a single array flow through a two-stage `FILTER` → `TRANSFORM` pipeline —
elements are dropped in stage 1 before stage 2 ever sees them, which is why
composing HOFs left-to-right is more efficient than transforming everything
and filtering afterward.

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

______________________________________________________________________

## :material-cart: All Three Together: Revenue From Active Line Items

`TRANSFORM`, `FILTER`, and `AGGREGATE` are usually composed, not used in isolation —
filter down to the rows that matter, transform each survivor to the value you actually
want, then fold those values into a single number, entirely inside one SQL expression
and without ever leaving row (per-order) grain:

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1, ARRAY(
        NAMED_STRUCT('sku','A1','status','ACTIVE',   'price',10.0,'quantity',2),
        NAMED_STRUCT('sku','A2','status','CANCELLED','price',5.0, 'quantity',1),
        NAMED_STRUCT('sku','A3','status','ACTIVE',   'price',7.5, 'quantity',3)
     )),
  (2, ARRAY(
        NAMED_STRUCT('sku','B1','status','ACTIVE','price',20.0,'quantity',1)
     ))
AS t(order_id, items);

SELECT
    order_id,
    AGGREGATE(
        TRANSFORM(
            FILTER(items, x -> x.status = 'ACTIVE'),   -- drop cancelled lines
            x -> x.price * x.quantity                  -- line total per surviving item
        ),
        0D,
        (acc, x) -> acc + x                            -- fold line totals into order revenue
    ) AS active_revenue
FROM orders;
-- order_id | active_revenue
-- ---------|---------------
-- 1        | 42.5   (A1: 10*2=20, A3: 7.5*3=22.5 → 42.5; A2 excluded)
-- 2        | 20.0
```

Verified against Spark 4.2 — cancelled item `A2` correctly contributes nothing to
`order_1`'s total. This three-stage `FILTER → TRANSFORM → AGGREGATE` chain replaces
what would otherwise require exploding `items` into rows, filtering, multiplying, and
re-aggregating with a `GROUP BY order_id` — all without a single `Generate`/shuffle
stage, because every array stays inside its own row the whole time.

______________________________________________________________________

## :material-swap-horizontal: Order Matters: Filter-First vs Transform-First

Filtering before transforming avoids running the (often more expensive) transform
lambda on elements that will be discarded anyway.

```sql
-- Less efficient: transforms every element, including ones later dropped
SELECT FILTER(
    TRANSFORM(prices, p -> ROUND(p * 1.08, 2)),   -- runs on ALL elements
    p -> p > 100
) AS taxed_high_prices;

-- More efficient: filters first, so TRANSFORM only touches surviving elements
SELECT TRANSFORM(
    FILTER(prices, p -> p > 100 / 1.08),          -- pre-filter narrows the set
    p -> ROUND(p * 1.08, 2)
) AS taxed_high_prices;
```

!!! tip "Same result, different cost"

    Both queries return the same rows — reordering `FILTER` before `TRANSFORM`
    changes only the *number of lambda invocations*, not the output.

______________________________________________________________________

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

______________________________________________________________________

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

______________________________________________________________________

## :material-state-machine: Sophisticated Algorithm: Sequential State Simulation

`AGGREGATE`'s accumulator can be an arbitrary `STRUCT`, not just a running number —
which means it can carry an entire evolving **state** through the array, one element
at a time, in order. This turns `AGGREGATE` into a general sequential-fold primitive:
enough to simulate a running account balance and flag every point it went negative,
purely in SQL, with no window function, no self-join, and no UDF:

```sql
CREATE OR REPLACE TEMP VIEW account_txns AS
SELECT * FROM VALUES
  ('acct1', ARRAY(100.0, -30.0, -90.0, 50.0, -20.0)),
  ('acct2', ARRAY(200.0, -50.0, -10.0))
AS t(account_id, txn_amounts);

SELECT
    account_id,
    AGGREGATE(
        txn_amounts,
        NAMED_STRUCT(
            'balance', CAST(0.0 AS DOUBLE),
            'overdraft_count', 0,
            'min_balance', CAST(0.0 AS DOUBLE)
        ),
        (acc, x) -> NAMED_STRUCT(
            'balance', acc.balance + x,
            'overdraft_count', acc.overdraft_count + IF(acc.balance + x < 0, 1, 0),
            'min_balance', LEAST(acc.min_balance, acc.balance + x)
        )
    ) AS simulation_result
FROM account_txns;
```

| account_id | simulation_result                                       |
| ---------- | ------------------------------------------------------- |
| acct1      | {balance: 10.0, overdraft_count: 1, min_balance: -20.0} |
| acct2      | {balance: 140.0, overdraft_count: 0, min_balance: 0.0}  |

Verified against Spark 4.2: `acct1`'s running total goes `100 → 70 → -20 → 30 → 10`,
correctly flagging exactly one overdraft (the `-90` transaction that pushed the
balance below zero) and tracking the lowest point reached (`-20`) — all inside a
single accumulator struct that's rebuilt once per array element, in order.

!!! note "Why this is more than a toy example"

    Each lambda invocation only sees the *previous* accumulator and the *current*
    element — never the whole array or its future elements — which is exactly the
    contract a fold/reduce needs to guarantee sequential, order-dependent state
    transitions. Anything expressible as "read one state, one event, produce the next
    state" (running balances, small state machines over an embedded event array,
    watermark/high-water-mark tracking, streak counters) can be written this way.
    Because `AGGREGATE` operates entirely within one row's array, it needs no shuffle,
    no window frame, and no self-join — the whole simulation is a single expression
    evaluated once per row.

!!! warning "Where this approach stops making sense"

    `AGGREGATE` folds left-to-right over an array **already embedded in the row** — it
    cannot reach across rows. If the "events" you need to fold live in separate rows
    (not already collected into an array column), first bring them together with
    `COLLECT_LIST` in a `GROUP BY` (sorted with `ARRAY_SORT` if order matters) before
    reaching for this pattern — or use window functions directly if a running total
    over table rows is all you need (see [Running Total](../../patterns/aggregation/running-total.md)).

______________________________________________________________________

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

______________________________________________________________________

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

______________________________________________________________________

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

______________________________________________________________________

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

______________________________________________________________________

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

______________________________________________________________________

## :material-speedometer: Performance Tips

| Tip                                                                       | Reason                                                    |
| ------------------------------------------------------------------------- | --------------------------------------------------------- |
| Pre-filter rows with `array_contains` before HOFs                         | `array_contains` can be pushed to file scans; HOFs cannot |
| Avoid deeply nested lambdas (3+ levels)                                   | Hard to read; consider LATERAL VIEW + inline instead      |
| Use `ARRAY_MIN` / `ARRAY_MAX` / `ARRAY_JOIN` over `AGGREGATE` equivalents | Native functions are faster                               |
| Cache the result of an expensive HOF in a CTE                             | Avoids re-computing the same HOF multiple times           |
| Prefer `SIZE(FILTER(...)) > 0` over `EXISTS(...)` for complex predicates  | Both are equivalent; `EXISTS` is slightly cleaner         |

______________________________________________________________________

## :material-alert-circle: Common Mistakes

| Mistake                                    | Result                                                       | Fix                                             |
| ------------------------------------------ | ------------------------------------------------------------ | ----------------------------------------------- |
| Using HOF result directly in `WHERE`       | Type mismatch (array, not bool)                              | Wrap with `SIZE(...) > 0` or `EXISTS`           |
| Lambda parameter name matches outer column | Silent shadowing                                             | Use unique parameter names                      |
| `ZIP_WITH` on unequal-length arrays        | **NULL-padded** to the longer array's length (not truncated) | Guard lambda with `COALESCE(x, default)`        |
| `AGGREGATE` on NULL array                  | Returns NULL                                                 | Guard: `COALESCE(AGGREGATE(...), default)`      |
| Calling a registered UDF inside a lambda   | Analysis error                                               | Rewrite as SQL expression or use `LATERAL VIEW` |
