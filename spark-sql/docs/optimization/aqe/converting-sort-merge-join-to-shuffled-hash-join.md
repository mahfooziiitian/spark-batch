# :material-shuffle-variant: AQE: Sort-Merge Join → Shuffled Hash Join

When broadcast is disabled or undesirable, Spark 4.2 can still replace a
`SortMergeJoin` with `ShuffledHashJoin` if the per-partition build side is small enough.

______________________________________________________________________

## :material-sitemap: Overview

This adaptive rewrite keeps the shuffle but removes the two sorts from the final join
stage. The key switch is `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`.

### :material-animation-play: Interactive Visualization — Sort-Merge vs Shuffled Hash

<div id="viz-aqe-smj-shj" class="ts-viz"></div>

Move the per-partition build-side size relative to the local-map threshold to see when
AQE can replace the merge join with a shuffled hash join.

______________________________________________________________________

## :material-check-decagram: Verified Spark 4.2 Controls

| Property                                                  | Verified value | Why it matters                                             |
| --------------------------------------------------------- | -------------- | ---------------------------------------------------------- |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | `0b`           | Default disables the adaptive rewrite                      |
| `spark.sql.join.preferSortMergeJoin`                      | `true`         | Static preference is still for SMJ                         |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes`         | `67108864b`    | Often paired with the local-map threshold when testing SHJ |

______________________________________________________________________

## :material-flask-outline: Verified Runtime Conversion

PySpark 4.2.0 was run with:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.autoBroadcastJoinThreshold = -1;
SET spark.sql.adaptive.autoBroadcastJoinThreshold = -1;
SET spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold = 1048576;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 1048576;
SET spark.sql.shuffle.partitions = 8;
```

Test shape:

- Left side: `range(500000)` projected to `k = id % 100000`
- Right side: a 5,000-row `ExistingRDD`
- Initial plan: `SortMergeJoin`
- Final plan after `collect()` on the same DataFrame: `ShuffledHashJoin`

Verified final-plan excerpt:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage
   +- Project
      +- ShuffledHashJoin [k], [k], Inner, BuildRight
         :- AQEShuffleRead coalesced
         +- AQEShuffleRead coalesced
+- == Initial Plan ==
   Project
   +- SortMergeJoin [k], [k], Inner
```

Spark reported right-side shuffle statistics of about `117.2 KiB` and `rowCount=5000`,
which was small enough for the configured 1 MiB local-map threshold.

______________________________________________________________________

## :material-alert-circle-outline: Behavior Gotchas

1. **This is opt-in in Spark 4.2** — the verified default is still `0b`, which disables the rewrite.
2. **The threshold is per shuffled partition, not total table size** — a 5 MiB table can still qualify if each build-side partition is small enough.
3. **You usually disable broadcast when demonstrating SHJ** — otherwise AQE may choose BHJ first.
4. **AQE can override the static SMJ preference** — even with `spark.sql.join.preferSortMergeJoin=true`, the final plan switched to `ShuffledHashJoin` once the runtime condition was satisfied.

______________________________________________________________________

## :material-compare: SMJ vs SHJ vs BHJ

| Feature               | Sort-Merge Join  | Shuffled Hash Join                     | Broadcast Hash Join                   |
| --------------------- | ---------------- | -------------------------------------- | ------------------------------------- |
| Full shuffle          | Yes              | Yes                                    | No final shuffle join stage           |
| Sort required         | Yes              | No                                     | No                                    |
| Build-side memory     | Low              | Per-partition hash map                 | Broadcast copy on executors           |
| Verified trigger here | Default fallback | `maxShuffledHashJoinLocalMapThreshold` | `adaptive.autoBroadcastJoinThreshold` |

______________________________________________________________________

## :material-brain: When to Use

| Scenario                                                   | Recommendation     |
| ---------------------------------------------------------- | ------------------ |
| Broadcast is too expensive but sorts dominate runtime      | Try adaptive SHJ   |
| Build-side partitions are comfortably below 64 MiB or less | Good SHJ candidate |
| Full outer join or large skew risk                         | Prefer SMJ         |
