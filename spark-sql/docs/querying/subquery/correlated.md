# :material-arrow-decision: Correlated Subqueries

A correlated subquery references columns from the outer query. Logically that means the inner query depends on each outer row or group, but in Spark 4.2 Catalyst can often decorrelate the pattern into a join.

<script src="../../assets/js/querying-subquery-viz.js"></script>

### :material-animation-play: Interactive Visualization — Decorrelated vs Nested Loop Plan

<div id="viz-subquery-correlated-plan" class="ts-viz"></div>

Switch between verified plan shapes to see when Spark 4.2 uses a hash join and when it has to fall back to a nested-loop style join.

______________________________________________________________________

## :material-code-tags: Typical Patterns

```sql
SELECT order_id, customer, amount
FROM orders o
WHERE amount > (
    SELECT AVG(amount)
    FROM orders i
    WHERE i.customer = o.customer
);
```

```sql
SELECT name
FROM customers c
WHERE EXISTS (
    SELECT 1
    FROM customer_orders o
    WHERE o.customer_id = c.customer_id
);
```

______________________________________________________________________

## :material-information-outline: Verified Spark 4.2 Behavior

1. Correlated **equality** predicates decorrelate well. The customer-average example above became a `BroadcastHashJoin` in the physical plan.
2. Correlated scalar subqueries in a `SELECT` list can also decorrelate. A verified `COUNT(*)` example became a `LeftOuter` join.
3. Uncorrelated scalar subqueries are different: Spark keeps a `Subquery` node instead of rewriting them as joins.
4. Correlated **non-equality** predicates can still decorrelate, but often only into `BroadcastNestedLoopJoin`.
5. A correlated scalar subquery must still return at most one row for each outer row.

______________________________________________________________________

## :material-flask-outline: Verified Plans

### Equality correlation: join rewrite

Verified query:

```sql
SELECT order_id, customer, amount
FROM orders o
WHERE amount > (
    SELECT AVG(amount)
    FROM orders i
    WHERE i.customer = o.customer
);
```

Spark 4.2 optimized it to an inner join between `orders` and a per-customer aggregate, then used a `BroadcastHashJoin` physically.

### Correlated scalar in `SELECT`

```sql
SELECT
    customer,
    (SELECT COUNT(*) FROM orders i WHERE i.customer = o.customer) AS cnt
FROM (SELECT DISTINCT customer FROM orders) o;
```

Spark 4.2 rewrote this to a `LeftOuter` join.

### Non-equality correlation: heavier plan

```sql
SELECT order_id, customer, amount
FROM orders o
WHERE EXISTS (
    SELECT 1
    FROM orders i
    WHERE i.amount > o.amount
);
```

Spark 4.2 still removed the subquery node, but the physical plan used `BroadcastNestedLoopJoin` because the condition was non-equality.

______________________________________________________________________

## :material-swap-horizontal: What to Look for in `EXPLAIN`

| You see                                              | Meaning                                            |
| ---------------------------------------------------- | -------------------------------------------------- |
| `LeftSemi`, `LeftAnti`, `Inner`, or `LeftOuter` join | Spark decorrelated the subquery                    |
| `BroadcastHashJoin`                                  | Equality-based decorrelation succeeded efficiently |
| `BroadcastNestedLoopJoin`                            | Correlation stayed expensive                       |
| `Subquery subquery#...` in a filter                  | Usually an uncorrelated scalar subquery            |

______________________________________________________________________

## :material-alert-decagram: When It Doesn't Just "Decorrelate" — Runtime Failure

Not every correlated pattern degrades gracefully into a slower join; a correlated
scalar subquery that returns more than one row per outer row **fails at runtime**,
not at plan time — `EXPLAIN` looks fine, but execution errors out. Verified on Spark
4.2:

```sql
-- customer 'A' has 3 orders, so the subquery returns 3 rows for order_id=1's outer row
SELECT o.order_id,
       (SELECT i.amount FROM orders i WHERE i.customer = o.customer) AS other_amount
FROM orders o;
```

```text
org.apache.spark.SparkRuntimeException: [SCALAR_SUBQUERY_TOO_MANY_ROWS] More than
one row returned by a subquery used as an expression. SQLSTATE: 21000
```

The plan happily rewrites this to a join, but Catalyst can't verify at compile time
that each outer row matches at most one inner row — the check only happens at
execution, so this query can run correctly against clean data for months and then
crash the moment a customer gets a second order. **Rewrite it as a window function**
so the "pick one row" logic is explicit and guaranteed, instead of an implicit
one-row assumption:

```sql
SELECT order_id, amount AS other_amount
FROM (
    SELECT o.order_id,
           i.amount,
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY i.amount DESC) AS rn
    FROM orders o
    JOIN orders i ON i.customer = o.customer
)
WHERE rn = 1;
```

Or, if the intent was actually "the aggregate value for this customer" rather than
"an arbitrary row", make that explicit with `GROUP BY`/window aggregation instead —
which is exactly what already-supported patterns like `AVG`/`COUNT` correlated
subqueries do (see equality correlation above); the failure only shows up when the
correlated subquery returns a **bare column** with no aggregation to guarantee
cardinality.

### Correlated `OR` across two different columns: still a nested-loop join

```sql
SELECT o.order_id FROM orders o
WHERE EXISTS (
    SELECT 1 FROM orders i
    WHERE i.customer = o.customer OR i.amount = o.amount
);
```

```text
+- BroadcastNestedLoopJoin BuildRight, LeftSemi,
     ((customer#i = customer#o) OR (amount#i = amount#o))
```

Spark still decorrelates this into a `LeftSemi` join (it doesn't error), but because
the correlation condition is an `OR` across two different columns, no hash key can
represent it, so it falls back to `BroadcastNestedLoopJoin` — a full O(N×M) comparison.
**Rewrite as a `UNION` of two separately-equi-correlated `EXISTS`/joins** (or as an
explicit `UNION ALL` + `DISTINCT` join) so each branch can use a `BroadcastHashJoin`:

```sql
SELECT order_id FROM orders o WHERE EXISTS (SELECT 1 FROM orders i WHERE i.customer = o.customer)
UNION
SELECT order_id FROM orders o WHERE EXISTS (SELECT 1 FROM orders i WHERE i.amount = o.amount);
```

______________________________________________________________________

## :material-lightbulb-outline: When to Rewrite Manually

| Situation                                                | Advice                                                                                                                                                            |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Equality correlation on a grouped key                    | Let Spark try first                                                                                                                                               |
| Non-equality correlation                                 | Prefer an explicit rewrite when possible                                                                                                                          |
| Large-table correlated scalar in `SELECT`                | Consider a pre-aggregated join                                                                                                                                    |
| Multi-row scalar risk                                    | Aggregate or rewrite before production                                                                                                                            |
| Scalar subquery with no aggregation guaranteeing one row | Rewrite as `ROW_NUMBER()`/window function to make the "pick one row" logic explicit — don't rely on unverified single-row assumptions (see runtime failure above) |
| Correlated `OR` spanning two different columns           | Split into a `UNION` of two equi-correlated `EXISTS`/joins so each side can use a hash join instead of `BroadcastNestedLoopJoin`                                  |
