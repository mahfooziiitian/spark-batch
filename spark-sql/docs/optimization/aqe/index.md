# :material-lightning-bolt: Adaptive Query Execution (AQE)

**Adaptive Query Execution (AQE)** lets Spark change parts of the physical plan
after shuffle statistics are available. In Spark 4.2 this is enabled by default,
so the important question is usually not *whether* AQE is on, but *which adaptive
rewrite actually fired*.

!!! note "Related"

    This page is the landing page for AQE defaults and tuning. For a plan-reading
    walkthrough with verified initial-versus-final output, continue to
    [AQE Deep Dive](spark-sql-aqe.md).

______________________________________________________________________

## :material-sitemap: Overview

AQE re-optimises only at stage boundaries. That means Spark keeps the logical query,
waits for shuffle-map statistics, and then decides whether later stages should read
those shuffle files differently or switch join strategy entirely.

### :material-animation-play: Interactive Visualization — AQE Decision Points

<div id="viz-aqe-overview" class="ts-viz"></div>

This view shows where AQE can step in: after shuffle statistics are materialised,
Spark may coalesce partitions, switch join strategy, or split skewed reads.

______________________________________________________________________

## :material-check-decagram: Verified Spark 4.2 Defaults

The values below were checked with PySpark 4.2.0 via `spark.conf.get(...)`.

| Property                                                      | Verified default | Notes                                                                    |
| ------------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------ |
| `spark.sql.adaptive.enabled`                                  | `true`           | AQE master switch                                                        |
| `spark.sql.adaptive.coalescePartitions.enabled`               | `true`           | Coalesces small post-shuffle partitions                                  |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst`      | `true`           | Favors task parallelism over advisory size                               |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize`      | `1048576b`       | 1 MiB floor                                                              |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum`   | `None`           | Unset by default; AQE starts from the exchange width already in the plan |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes`             | `67108864b`      | 64 MiB advisory target                                                   |
| `spark.sql.adaptive.skewJoin.enabled`                         | `true`           | Join-side skew handling                                                  |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor`           | `5.0`            | Compared to median partition size                                        |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `268435456b`     | 256 MiB absolute threshold                                               |
| `spark.sql.adaptive.localShuffleReader.enabled`               | `true`           | Enables `AQEShuffleRead local` after some rewrites                       |
| `spark.sql.autoBroadcastJoinThreshold`                        | `10485760b`      | 10 MiB static broadcast cutoff                                           |
| `spark.sql.adaptive.autoBroadcastJoinThreshold`               | `None`           | AQE inherits the regular broadcast threshold unless you override it      |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`     | `0b`             | Disables adaptive SMJ → SHJ conversion until set                         |

______________________________________________________________________

## :material-compare: What AQE Can Change at Runtime

| Runtime rewrite                                                                 | Verified Spark 4.2 marker                                  | Why it helps                                                |
| ------------------------------------------------------------------------------- | ---------------------------------------------------------- | ----------------------------------------------------------- |
| [Partition coalescing](coalescing-post-shuffle-partitions.md)                   | `AQEShuffleRead coalesced`                                 | Fewer tiny tasks and fewer tiny files                       |
| [SMJ → Broadcast Hash Join](converting-sort-merge-join-broadcast-join.md)       | `BroadcastHashJoin` in `== Final Plan ==`                  | Removes the sort-merge work when one side is actually small |
| [SMJ → Shuffled Hash Join](converting-sort-merge-join-to-shuffled-hash-join.md) | `ShuffledHashJoin` in `== Final Plan ==`                   | Keeps the shuffle but skips both sorts                      |
| [Skew join optimisation](optimizing-skew-join.md)                               | `SortMergeJoin(skew=true)` and `AQEShuffleRead ... skewed` | Breaks up a hot reducer into smaller reads                  |
| [Skewed rebalance read splitting](splitting-skewed-shuffle-partitions.md)       | `AQEShuffleRead coalesced and skewed` after `REBALANCE`    | Smooths write-side chunk sizes under AQE                    |

______________________________________________________________________

## :material-flask-outline: Verified Quick Checks

```sql
SET spark.sql.adaptive.enabled = true;

EXPLAIN FORMATTED
SELECT region, SUM(amount)
FROM sales
GROUP BY region;
```

What to look for in Spark 4.2:

- Before execution: `AdaptiveSparkPlan ... isFinalPlan=false`
- After executing the **same** DataFrame: `== Final Plan ==` and `isFinalPlan=true`
- Coalescing: `AQEShuffleRead coalesced`
- Runtime broadcast conversion: `BroadcastQueryStage` + `BroadcastHashJoin`
- Skew handling: `AQEShuffleRead ... skewed`

______________________________________________________________________

## :material-alert-circle-outline: Behavior Gotchas

1. **Final plans are post-execution artifacts** — `EXPLAIN FORMATTED` on an unexecuted query only shows the initial adaptive wrapper.
2. **`initialPartitionNum` is not literally `spark.sql.shuffle.partitions` by default** — the config is unset (`None`), although most plans still start from the shuffle width already chosen in the exchange.
3. **Spark 4.2 plan text says `AQEShuffleRead`, not `CustomShuffleReaderExec`** in formatted output.
4. **AQE is not join reordering** — it changes *how* later stages run, not the logical join tree itself.

______________________________________________________________________

## :material-brain: When to Tune AQE Explicitly

| Symptom                                                      | Verified knob to inspect first                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------- |
| Thousands of small output files                              | `coalescePartitions.parallelismFirst`, `advisoryPartitionSizeInBytes`           |
| Sort-merge joins surviving despite a tiny runtime build side | `adaptive.autoBroadcastJoinThreshold`, `nonEmptyPartitionRatioForBroadcastJoin` |
| Sort cost dominates but broadcast is too expensive           | `maxShuffledHashJoinLocalMapThreshold`                                          |
| One reducer lags far behind the rest                         | `skewJoin.*` for joins, `REBALANCE` + rebalance skew settings for write shaping |

______________________________________________________________________

## :material-book-open-variant: In This Section

| Page                                                                            | Focus                                          |
| ------------------------------------------------------------------------------- | ---------------------------------------------- |
| [AQE Deep Dive](spark-sql-aqe.md)                                               | Initial vs final plan reading                  |
| [Coalescing Post-Shuffle Partitions](coalescing-post-shuffle-partitions.md)     | Verified `AQEShuffleRead coalesced` behavior   |
| [SMJ → Broadcast Hash Join](converting-sort-merge-join-broadcast-join.md)       | Runtime broadcast conversion                   |
| [SMJ → Shuffled Hash Join](converting-sort-merge-join-to-shuffled-hash-join.md) | Runtime hash-join conversion without broadcast |
| [Optimizing Skew Join](optimizing-skew-join.md)                                 | Join skew detection and splitting              |
| [Splitting Skewed Shuffle Partitions](splitting-skewed-shuffle-partitions.md)   | AQE skew handling on `REBALANCE` reads         |
