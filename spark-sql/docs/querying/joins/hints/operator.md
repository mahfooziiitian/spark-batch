# :material-lightbulb-on: Spark SQL Join Hint Operators

Spark 4.2 recognizes several join-strategy hints and a few alias spellings for them. The examples below focus on what `EXPLAIN FORMATTED` actually showed in PySpark 4.2.

### :material-animation-play: Interactive Visualization — Hint Operator Cheatsheet

<div id="viz-join-hint-operators-core" class="ts-viz"></div>

Choose a hint family to see its accepted alias names and the physical operator it produced during verification.

<script src="../../../assets/js/querying-joins-core-viz.js"></script>

______________________________________________________________________

## :material-rocket-launch: `BROADCAST`

Broadcasts one side to every executor so Spark can build a hash join without shuffling both sides.

```sql
SELECT /*+ BROADCAST(dim) */
    f.order_id,
    dim.region
FROM fact_orders AS f
JOIN dim_region AS dim
    ON f.region_id = dim.id;
```

Verified aliases in PySpark 4.2:

- `BROADCAST(dim)`
- `BROADCASTJOIN(dim)`
- `MAPJOIN(dim)`

All three produced `BroadcastHashJoin` for the tested equi join, even with `spark.sql.autoBroadcastJoinThreshold = -1`.

!!! note "Outer-join limitation"

    In the verification run, `FULL OUTER JOIN` with `BROADCAST` did not stay broadcast-based; Spark planned `SortMergeJoin` instead.

______________________________________________________________________

## :material-sort: `MERGE`

Requests a sort-merge join.

```sql
SELECT /*+ MERGE(orders) */
    o.order_id,
    p.payment_status
FROM orders AS o
JOIN payments AS p
    ON o.order_id = p.order_id;
```

Verified aliases in PySpark 4.2:

- `MERGE(orders)`
- `SHUFFLE_MERGE(orders)`
- `MERGEJOIN(orders)`

Each produced `SortMergeJoin` in the test run.

______________________________________________________________________

## :material-shuffle-variant: `SHUFFLE_HASH`

Requests a shuffle hash join.

```sql
SELECT /*+ SHUFFLE_HASH(dim) */
    f.sale_id,
    dim.category
FROM fact_sales AS f
JOIN dim_product AS dim
    ON f.product_id = dim.product_id;
```

The verified PySpark 4.2 plan used `ShuffledHashJoin` for the tested equi join.

______________________________________________________________________

## :material-grid: `SHUFFLE_REPLICATE_NL`

Requests shuffle-and-replicate nested-loop execution.

```sql
SELECT /*+ SHUFFLE_REPLICATE_NL(dates) */
    p.product_id,
    d.date_value
FROM products AS p
JOIN dates AS d
    ON p.calendar_id = d.calendar_id;
```

In the PySpark 4.2 verification run, `EXPLAIN FORMATTED` surfaced this choice as `CartesianProduct` for the tested inner join. That is expected: Spark is no longer using a hash or sort-merge equi strategy.

!!! warning "Use sparingly"

    Nested-loop and cartesian-style plans scale poorly. Reserve this hint for cases where you understand the fan-out and the other strategies are not appropriate.

______________________________________________________________________

## :material-order-bool-descending: Operator Priority

If different sides of the same join request different strategies, Spark resolves them in this order:

| Priority | Hint family            | Verified winner             |
| -------- | ---------------------- | --------------------------- |
| 1        | `BROADCAST`            | Beat `MERGE`                |
| 2        | `MERGE`                | Beat `SHUFFLE_HASH`         |
| 3        | `SHUFFLE_HASH`         | Beat `SHUFFLE_REPLICATE_NL` |
| 4        | `SHUFFLE_REPLICATE_NL` | Lowest priority             |

______________________________________________________________________

## :material-code-tags: Verification Pattern

```sql
EXPLAIN FORMATTED
SELECT /*+ BROADCASTJOIN(dim) */
    f.order_id,
    dim.region
FROM fact_orders AS f
JOIN dim_region AS dim
    ON f.region_id = dim.id;
```

Check the physical section for `BroadcastHashJoin`, `SortMergeJoin`, `ShuffledHashJoin`, or `CartesianProduct`.
