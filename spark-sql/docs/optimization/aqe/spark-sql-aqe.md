# :material-auto-fix: AQE Deep Dive

This page focuses on **how to read AQE in Spark 4.2 plans** rather than just listing
the feature switches. The key habit is to compare the initial adaptive shell with the
final executed plan for the exact same query object.

!!! note "Related"

    For the default values and tuning matrix, start with the
    [AQE overview](index.md). This page goes deeper on plan anatomy.

______________________________________________________________________

## :material-layers: Query Stage Model

AQE only makes decisions after a shuffle query stage finishes. That is why Spark can
change the *downstream* readers and joins, but not a stage that is already running.

### :material-animation-play: Interactive Visualization — Initial Plan vs Final Plan

<div id="viz-aqe-final-plan-toggle" class="ts-viz"></div>

Toggle between the pre-execution and post-execution views to see how Spark 4.2 keeps
both plan versions under `AdaptiveSparkPlan`.

______________________________________________________________________

## :material-check-decagram: Verified Spark 4.2 Example

The following PySpark 4.2 query was executed with:

- `spark.sql.autoBroadcastJoinThreshold = -1`
- `spark.sql.adaptive.autoBroadcastJoinThreshold = 10485760`
- `spark.sql.shuffle.partitions = 8`

That forced the **initial** plan to pick `SortMergeJoin`, then allowed AQE to switch
only after runtime shuffle statistics were available.

Initial plan before execution:

```text
== Physical Plan ==
AdaptiveSparkPlan ... isFinalPlan=false
+- SortMergeJoin
   :- Sort
   :  +- Exchange hashpartitioning(k, 8)
   +- Sort
      +- Exchange hashpartitioning(k, 8)
```

Final plan after executing the same DataFrame and calling `explain("formatted")` again:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage
   +- Project
      +- BroadcastHashJoin Inner BuildRight
         :- AQEShuffleRead local
         :  +- ShuffleQueryStage
         +- BroadcastQueryStage
            +- BroadcastExchange
               +- AQEShuffleRead local
+- == Initial Plan ==
   Project
   +- SortMergeJoin
```

This verifies two important behaviors in Spark 4.2:

1. `AdaptiveSparkPlan` preserves both the initial and final physical plans.
2. The post-AQE formatted plan uses `AQEShuffleRead local` rather than the older `LocalShuffleReaderExec` wording.

______________________________________________________________________

## :material-code-json: Reading Spark 4.2 AQE Markers

| Marker                                    | Verified meaning                                              |
| ----------------------------------------- | ------------------------------------------------------------- |
| `AdaptiveSparkPlan ... isFinalPlan=false` | Query has not yet produced a final adaptive rewrite           |
| `AdaptiveSparkPlan ... isFinalPlan=true`  | The executed query object now carries the final adaptive plan |
| `ShuffleQueryStage`                       | A shuffle stage whose statistics are now known                |
| `BroadcastQueryStage`                     | AQE materialized a broadcast relation from a completed stage  |
| `AQEShuffleRead coalesced`                | Spark merged adjacent small shuffle partitions                |
| `AQEShuffleRead local`                    | Spark can read shuffle blocks locally after a runtime rewrite |
| `AQEShuffleRead ... skewed`               | AQE split a skewed read path                                  |

______________________________________________________________________

## :material-toggle-switch: Selective Controls

You can keep AQE enabled while disabling specific rewrites:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = false;
SET spark.sql.adaptive.skewJoin.enabled = false;
```

For runtime broadcast conversion, Spark 4.2 also distinguishes between the static and
adaptive thresholds:

```sql
SET spark.sql.autoBroadcastJoinThreshold = -1;
SET spark.sql.adaptive.autoBroadcastJoinThreshold = 10485760;
```

That combination is useful when you want to **demonstrate** AQE switching an initial
sort-merge plan to a broadcast final plan.

______________________________________________________________________

## :material-alert-circle-outline: Behavior Gotchas

1. **Execute the exact query object you plan to inspect** — running a wrapper query such as a separate `count(*)` can leave the original object at `isFinalPlan=false`.
2. **AQE does not rewrite mid-stage** — all changes happen between completed query stages.
3. **The initial exchange width still matters** — AQE can coalesce downward, but it needs a shuffle boundary to adapt at all.
4. **Join-order mistakes remain join-order mistakes** — AQE can improve the chosen join strategy, but it does not reorder a multi-join tree.

______________________________________________________________________

## :material-compare: Static vs Adaptive Planning

| Aspect          | Static plan             | AQE final plan                               |
| --------------- | ----------------------- | -------------------------------------------- |
| Join strategy   | Fixed before execution  | Can switch after stage statistics arrive     |
| Partition count | Stays at exchange width | Can be coalesced on the read side            |
| Skew handling   | Manual only             | Can split skewed reads automatically         |
| Explain output  | Single physical plan    | `== Final Plan ==` plus `== Initial Plan ==` |
