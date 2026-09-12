# :material-lightbulb-on: Join Hints

Join hints let you bias Spark's physical join selection. PySpark 4.2 confirmed that the core join-strategy hints below do change the physical operator for equi joins when the strategy is supported.

### :material-animation-play: Interactive Visualization — Hint Alias Mapper

<div id="viz-join-hints-core" class="ts-viz"></div>

Pick a hint alias to see the physical join operator that `EXPLAIN FORMATTED` showed during PySpark 4.2 verification.

<script src="../../../assets/js/querying-joins-core-viz.js"></script>

______________________________________________________________________

## :material-table: Verified Hint Reference

| Hint syntax                       | Verified physical operator in PySpark 4.2   | Notes                                                                                             |
| --------------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `BROADCAST(t)`                    | `BroadcastHashJoin`                         | `BROADCASTJOIN(t)` and `MAPJOIN(t)` behaved the same                                              |
| `MERGE(t)`                        | `SortMergeJoin`                             | `SHUFFLE_MERGE(t)` and `MERGEJOIN(t)` behaved the same                                            |
| `SHUFFLE_HASH(t)`                 | `ShuffledHashJoin`                          | Forces shuffle-hash selection for the tested equi join                                            |
| `SHUFFLE_REPLICATE_NL(t)`         | `CartesianProduct`                          | `EXPLAIN FORMATTED` exposed the nested-loop family as `CartesianProduct` in the tested inner join |
| `[Databricks] RANGE_JOIN(t, bin)` | No change in open-source Spark 4.2          | See [range-join-hint.md](range-join-hint.md); the tested OSS plan stayed `CartesianProduct`       |
| `[Databricks] SKEW(...)`          | No strategy change in open-source Spark 4.2 | Not an OSS join-strategy hint in the verification run                                             |

______________________________________________________________________

## :material-pencil-outline: Syntax

```sql
SELECT /*+ BROADCAST(dim) */
    f.order_id,
    dim.region
FROM fact_orders AS f
JOIN dim_region AS dim
    ON f.region_id = dim.id;
```

```sql
SELECT /*+ MERGE(a), SHUFFLE_HASH(b) */ *
FROM large_a AS a
JOIN large_b AS b
    ON a.id = b.id;
```

Hints target the relation name or alias that is visible at that point in the query.

______________________________________________________________________

## :material-sort-numeric-ascending: Verified Precedence

When both sides of the same join carry conflicting strategy hints, PySpark 4.2 matched Spark's documented priority order:

1. `BROADCAST`
2. `MERGE`
3. `SHUFFLE_HASH`
4. `SHUFFLE_REPLICATE_NL`

Verified examples:

- `BROADCAST(left_t)` + `MERGE(right_t)` -> `BroadcastHashJoin`
- `MERGE(left_t)` + `SHUFFLE_HASH(right_t)` -> `SortMergeJoin`
- `SHUFFLE_HASH(left_t)` + `SHUFFLE_REPLICATE_NL(right_t)` -> `ShuffledHashJoin`

When both sides were hinted with `BROADCAST`, the symmetric test query built on the right side. That build-side choice can still vary with join type and statistics.

______________________________________________________________________

## :material-magnify: Behavior Notes

1. Hints influence physical planning; they do not change join semantics.
2. Use the alias actually present in the query. A hint attached to an out-of-scope or wrong relation name is ignored.
3. Unsupported combinations can fall back. In the verification run, `FULL OUTER JOIN` plus `BROADCAST` still planned as `SortMergeJoin`.
4. `EXPLAIN FORMATTED` is the fastest way to confirm whether Spark honored the hint.

______________________________________________________________________

## :material-code-tags: Verify with `EXPLAIN FORMATTED`

```sql
EXPLAIN FORMATTED
SELECT /*+ SHUFFLE_HASH(dim) */
    f.sale_id,
    dim.category
FROM fact_sales AS f
JOIN dim_product AS dim
    ON f.product_id = dim.product_id;
```
