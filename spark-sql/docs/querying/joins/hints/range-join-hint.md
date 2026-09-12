# :material-lightbulb-on: [Databricks] Range Join Hint

Databricks documents a range-join optimization based on the `RANGE_JOIN` hint and bin sizing. Open-source Spark 4.2 does **not** implement that optimization: the same SQL hint was accepted syntactically in PySpark 4.2 but did not change the physical plan.

### :material-animation-play: Interactive Visualization — OSS vs Databricks Range Join Story

<div id="viz-join-range-hint-core" class="ts-viz"></div>

Switch between open-source Spark 4.2 and Databricks guidance to see which parts are portable and which parts are platform-specific.

<script src="../../../assets/js/querying-joins-core-viz.js"></script>

______________________________________________________________________

## :material-alert: Verified Open-Source Spark 4.2 Behavior

Tested query:

```sql
SELECT /*+ RANGE_JOIN(ranges, 10) */ *
FROM points
JOIN ranges
    ON points.p >= ranges.start
   AND points.p < ranges.end;
```

Verified outcome in PySpark 4.2:

- `EXPLAIN FORMATTED` still showed `CartesianProduct` for the tested non-equi join.
- Setting `spark.databricks.optimizer.rangeJoin.binSize = 5` did **not** change the OSS physical plan.
- In other words, the Databricks-specific hint name and config key are not an open-source Spark 4.2 optimization feature.

!!! warning "Portable documentation rule"

    If you are writing for open-source Spark, do not present `RANGE_JOIN` as an available join strategy. Label it `[Databricks]` and verify the target runtime before recommending it.

______________________________________________________________________

## :material-database: [Databricks] Documented Syntax

On Databricks Runtime, the documented pattern is:

```sql
SELECT /*+ RANGE_JOIN(points, 10) */ *
FROM points
JOIN ranges
    ON points.p >= ranges.start
   AND points.p < ranges.end;
```

Databricks documentation also describes a bin-size setting:

```sql
SET spark.databricks.optimizer.rangeJoin.binSize = 5;
```

This page keeps that syntax for reference, but it is **Databricks-only**.

______________________________________________________________________

## :material-chart-bar: When the Hint Is Conceptually Useful

Range-join optimization targets predicates such as:

- Point-in-interval joins: `p >= start AND p < end`
- Interval-overlap joins: `l.start < r.end AND r.start < l.end`

Those predicates are valid in plain Spark SQL, but open-source Spark 4.2 treats them as ordinary non-equi joins and plans them with its standard strategy selection.

______________________________________________________________________

## :material-lightbulb-outline: Portable Alternatives

For open-source Spark workloads, prefer techniques that remain valid everywhere:

1. Keep an equi key in the join if one exists.
2. Pre-filter both sides so the non-equi join sees fewer rows.
3. Bucket or discretize ranges in upstream data if your workload naturally supports it.
4. Use `EXPLAIN FORMATTED` to confirm whether the rewrite created an equi join that Catalyst can optimize.

______________________________________________________________________

## :material-link-variant: Reference

Databricks documents this feature in its SQL hints reference under `RANGE_JOIN`.
