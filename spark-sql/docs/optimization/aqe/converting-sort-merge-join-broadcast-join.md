# :material-broadcast: AQE: Sort-Merge Join → Broadcast Hash Join

Spark 4.2 can start with a `SortMergeJoin`, execute the shuffle stages, and then replace
the join with `BroadcastHashJoin` when the runtime build side is small enough.

______________________________________________________________________

## :material-sitemap: Overview

This adaptive rewrite is easiest to demonstrate when you disable **static** broadcast
planning but keep the **adaptive** broadcast threshold enabled.

### :material-animation-play: Interactive Visualization — Runtime Broadcast Decision

<div id="viz-aqe-smj-bhj" class="ts-viz"></div>

Adjust the runtime build-side size to see when Spark can keep the initial sort-merge
shell versus when it can switch to a broadcast final plan.

______________________________________________________________________

## :material-check-decagram: Verified Spark 4.2 Controls

| Property                                                    | Verified value | Notes                                                                            |
| ----------------------------------------------------------- | -------------- | -------------------------------------------------------------------------------- |
| `spark.sql.autoBroadcastJoinThreshold`                      | `10485760b`    | Static planner cutoff                                                            |
| `spark.sql.adaptive.autoBroadcastJoinThreshold`             | `None`         | AQE inherits the static cutoff unless you override it                            |
| `spark.sql.adaptive.localShuffleReader.enabled`             | `true`         | Produces `AQEShuffleRead local` in the final plan                                |
| `spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin` | `0.2`          | AQE may skip conversion if the would-be build side has too many empty partitions |

______________________________________________________________________

## :material-flask-outline: Verified Runtime Conversion

PySpark 4.2.0 was run with:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.autoBroadcastJoinThreshold = -1;
SET spark.sql.adaptive.autoBroadcastJoinThreshold = 10485760;
SET spark.sql.shuffle.partitions = 8;
```

Test shape:

- Left side: `range(500000)` projected to `k = id % 1000`
- Right side: a 100-row `ExistingRDD`, also keyed by `k`
- Initial plan: `SortMergeJoin`
- Final plan after `collect()` on the same DataFrame: `BroadcastHashJoin`

Verified final-plan excerpt:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage
   +- Project
      +- BroadcastHashJoin [k], [k], Inner, BuildRight
         :- AQEShuffleRead local
         :  +- ShuffleQueryStage
         +- BroadcastQueryStage
            +- BroadcastExchange
               +- AQEShuffleRead local
+- == Initial Plan ==
   Project
   +- SortMergeJoin [k], [k], Inner
```

The `ShuffleQueryStage` statistics for the right side were `rowCount=100`, and AQE
replaced the merge path after those runtime numbers became available.

______________________________________________________________________

## :material-alert-circle-outline: Behavior Gotchas

1. **Use the adaptive threshold when you want to prove the runtime switch** — setting only `autoBroadcastJoinThreshold=-1` disables broadcast entirely.
2. **The final Spark 4.2 marker is `AQEShuffleRead local`** — older prose that says `LocalShuffleReaderExec` is outdated for formatted plans.
3. **Sparse build sides may not convert** — with the default `nonEmptyPartitionRatioForBroadcastJoin=0.2`, a side that occupies too few reduce partitions can stay as SMJ even if it is tiny.
4. **AQE still needs the shuffle-stage stats first** — the conversion happens after Spark has already written the shuffle files for the initial plan.

______________________________________________________________________

## :material-compare: SMJ vs Runtime BHJ

| Aspect                | Initial `SortMergeJoin` | Final `BroadcastHashJoin`                          |
| --------------------- | ----------------------- | -------------------------------------------------- |
| Shuffle on both sides | Yes                     | Already written, but read via local/broadcast path |
| Sort on both sides    | Yes                     | Removed from the final join stage                  |
| Memory pressure       | Low during join         | Higher on the broadcast build side                 |
| Best fit              | Large × large           | Large × runtime-small                              |

______________________________________________________________________

## :material-brain: When to Tune

| Symptom                                      | Adjustment                                                                       |
| -------------------------------------------- | -------------------------------------------------------------------------------- |
| Small filtered dimension still uses SMJ      | Raise `adaptive.autoBroadcastJoinThreshold` or inspect non-empty partition ratio |
| Broadcast causes executor pressure           | Lower the adaptive threshold or force `MERGE`                                    |
| You want to demonstrate AQE behavior clearly | Disable static broadcast and leave adaptive broadcast enabled                    |
