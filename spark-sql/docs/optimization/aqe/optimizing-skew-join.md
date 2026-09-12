# :material-scale-unbalanced: AQE Skew Join Optimisation

AQE skew join handling is a **join-specific** rewrite. In Spark 4.2 it shows up in the
final plan as a skew-aware sort-merge join plus a skewed shuffle reader.

______________________________________________________________________

## :material-sitemap: Overview

Spark compares each reducer partition against both a median-based factor and an absolute
byte threshold. Only partitions that cross **both** limits are eligible for splitting.

### :material-animation-play: Interactive Visualization — Detecting a Skewed Join Partition

<div id="viz-aqe-skew-join" class="ts-viz"></div>

Adjust the hot partition relative to the median and absolute threshold to see when Spark
flags it as skewed enough to split.

______________________________________________________________________

## :material-check-decagram: Verified Spark 4.2 Defaults

| Property                                                      | Verified default | Notes                                    |
| ------------------------------------------------------------- | ---------------- | ---------------------------------------- |
| `spark.sql.adaptive.skewJoin.enabled`                         | `true`           | Join skew handling is enabled by default |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor`           | `5.0`            | Compared against the median reducer size |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `268435456b`     | 256 MiB absolute floor                   |
| `spark.sql.adaptive.forceOptimizeSkewedJoin`                  | `false`          | Useful only for forcing test scenarios   |

!!! note "Related"

    This page covers **join skew**. For skew handling on a `REBALANCE` exchange near a
    write path, see [Splitting Skewed Shuffle Partitions](splitting-skewed-shuffle-partitions.md).

______________________________________________________________________

## :material-flask-outline: Verified Spark 4.2 Skew Plan

A local PySpark 4.2.0 test used:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.forceOptimizeSkewedJoin = true;
SET spark.sql.autoBroadcastJoinThreshold = -1;
SET spark.sql.adaptive.autoBroadcastJoinThreshold = -1;
SET spark.sql.shuffle.partitions = 8;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 65536;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 131072;
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 2;
```

The skewed side used 40 upstream partitions so Spark had enough map-output boundaries to
split a hot reducer. After `collect()` on the joined DataFrame, the executed plan showed:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage
   +- Project
      +- SortMergeJoin(skew=true) [k], [k], Inner
         :- AQEShuffleRead coalesced and skewed
         :  +- ShuffleQueryStage
         +- AQEShuffleRead coalesced
            +- ShuffleQueryStage
+- == Initial Plan ==
   Project
   +- SortMergeJoin [k], [k], Inner
```

That is the clearest verified Spark 4.2 signature for AQE skew join handling.

______________________________________________________________________

## :material-compare: AQE Skew Handling vs Manual Salting

| Aspect                 | AQE skew join                         | Manual salting                            |
| ---------------------- | ------------------------------------- | ----------------------------------------- |
| Query rewrite required | No                                    | Yes                                       |
| Trigger basis          | Runtime shuffle stats                 | Manual design                             |
| Plan marker            | `SortMergeJoin(skew=true)`            | None; you infer it from the rewritten SQL |
| Best use               | Ordinary skew that appears at runtime | Extreme or persistent hot keys            |

!!! tip "Give AQE something to split"

    A join with only a couple of upstream map tasks gives AQE very little room to split a
    skewed reducer. The verified local test needed 40 input partitions before the skewed
    reader appeared reliably.

______________________________________________________________________

## :material-alert-circle-outline: Behavior Gotchas

1. **This is still a sort-merge join** — AQE makes the read path skew-aware; it does not turn skew handling into a different join family.
2. **Both skew conditions must be met** — factor-only or threshold-only is not enough.
3. **Spark 4.2 plan text uses `AQEShuffleRead ... skewed`** — not the older `CustomShuffleReaderExec` naming.
4. **Upstream partition granularity matters** — no extra map-output boundaries means little or nothing to split.

______________________________________________________________________

## :material-brain: When to Tune

| Symptom                                          | First move                                           |
| ------------------------------------------------ | ---------------------------------------------------- |
| One reducer dominates a join stage               | Lower the skew threshold or factor for that workload |
| AQE does not split locally reproducible skew     | Increase upstream partition count                    |
| A single key remains pathological even after AQE | Add targeted salting for that key                    |
