# :material-speedometer: Join Optimization

Join tuning in Spark 4.2 is mostly about steering Catalyst toward a cheaper physical operator, shrinking shuffle volume, and preventing a small number of hot keys from dominating one task.

### :material-animation-play: Interactive Visualization — Join Optimization Toolkit

<div id="viz-joins-optimization-overview" class="ts-viz"></div>

Compare the main optimization levers and see which ones change the operator, which ones reduce shuffle, and which ones mainly reduce repeat work.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified Levers From This Section

| Lever                | What was verified                                                                                                                                   |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Broadcast sizing     | Lowering `spark.sql.autoBroadcastJoinThreshold` from default to `-1` changed a tested equi-join from `BroadcastHashJoin` to `SortMergeJoin`.        |
| Shuffle-hash hinting | `SHUFFLE_HASH` produced `ShuffledHashJoin`; `preferSortMergeJoin=false` alone did not force it in local tests.                                      |
| Bucketing            | Matching bucketed tables joined without `Exchange` nodes in the physical plan, although Spark still inserted `Sort` nodes.                          |
| Salting              | A salted-key plus `explode(sequence(...))` expansion on the other side returned the same rows as the unsalted baseline join in a PySpark 4.2 check. |
| Iterative broadcast  | Manual chunked broadcast passes reproduced the same row count as a single broadcast join in a small verification example.                           |

______________________________________________________________________

## :material-tune: Start With the Cheapest Fix

1. **Broadcast a truly small side** for equi-joins.
2. **Filter before joining** so fewer rows reach shuffle.
3. **Use AQE skew handling** for skewed shuffle joins.
4. **Salt or separate hot keys** only when automatic techniques are not enough.
5. **Cache** only if the expensive intermediate is reused.

______________________________________________________________________

## :material-cog-outline: Settings That Matter Most

```sql
set spark.sql.autoBroadcastJoinThreshold = 10485760;
set spark.sql.join.preferSortMergeJoin = true;
set spark.sql.adaptive.enabled = true;
set spark.sql.adaptive.skewJoin.enabled = true;
set spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;
set spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456;
```

______________________________________________________________________

## :material-alert-outline: Avoid Common Myths

- Caching does not fix skew by itself.
- `preferSortMergeJoin=false` does not mean Spark must use `ShuffledHashJoin`.
- Bucketing can remove shuffle, but it does not guarantee Spark can also skip sorting.
- Names such as BMPJ or iterative broadcast describe manual patterns, not built-in Spark operators.

______________________________________________________________________

## :material-lightbulb-outline: How to Validate Your Change

Use `EXPLAIN FORMATTED` before and after a tuning change. If the operator, shuffle boundary, or number of repeated scans did not change, the optimization probably did not address the real bottleneck.
