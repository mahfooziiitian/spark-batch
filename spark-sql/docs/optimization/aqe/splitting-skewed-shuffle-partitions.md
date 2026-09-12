# :material-call-split: Splitting Skewed Shuffle Partitions

In Spark 4.2, the clearest non-join example of skewed shuffle read splitting is a
`REBALANCE` exchange under AQE. That is narrower and more accurate than saying the
feature applies to *any* shuffle stage.

!!! note "Correction"

    The previous wording on this page overstated the scope. The verified Spark 4.2 test
    below showed skewed read splitting after `REBALANCE`, controlled by rebalance-specific
    AQE settings — not by the `skewJoin.*` knobs alone.

______________________________________________________________________

## :material-sitemap: Overview

`REBALANCE` inserts a shuffle that AQE can reshape on the read side. When one rebalance
partition is much larger than the rest, Spark can both coalesce tiny neighbors and split
the oversized partition into smaller chunks.

### :material-animation-play: Interactive Visualization — Splitting a Skewed Rebalance Partition

<div id="viz-aqe-skew-split" class="ts-viz"></div>

This animation follows one oversized rebalance partition being cut into smaller reads
while small neighbors are still eligible for coalescing.

______________________________________________________________________

## :material-check-decagram: Verified Spark 4.2 Controls

| Property                                                        | Verified default | Role                                         |
| --------------------------------------------------------------- | ---------------- | -------------------------------------------- |
| `spark.sql.adaptive.enabled`                                    | `true`           | AQE must be on                               |
| `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` | `true`           | Enables skew optimization on rebalance reads |
| `spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor`    | `0.2`            | Helps merge very small rebalance partitions  |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes`               | `67108864b`      | Target size for rebalance outputs            |

!!! note "Not the same as join skew"

    `spark.sql.adaptive.skewJoin.enabled` governs join skew handling.
    The verified rebalance plan on this page was driven by `REBALANCE` plus the
    rebalance-specific AQE switch above.

______________________________________________________________________

## :material-flask-outline: Verified Spark 4.2 Example

PySpark 4.2.0 was run with:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.shuffle.partitions = 8;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 65536;
SET spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled = true;
```

Query shape:

```sql
SELECT /*+ REBALANCE(8, k) */
    k,
    pad
FROM source_data;
```

`source_data` was built so that most rows shared `k = 0`, making one rebalance output far
larger than the others. After executing the query, Spark 4.2 produced:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage
   +- AQEShuffleRead coalesced and skewed
      +- ShuffleQueryStage
         +- Exchange hashpartitioning(k, 8), REBALANCE_PARTITIONS_BY_COL
+- == Initial Plan ==
   Exchange hashpartitioning(k, 8), REBALANCE_PARTITIONS_BY_COL
```

This verifies that the skew-aware split happened on the **read side of the rebalance
exchange**, not as a generic rewrite for arbitrary shuffle operators.

______________________________________________________________________

## :material-compare: Join Skew vs Rebalance Skew Splitting

| Aspect               | Join skew optimisation                   | Rebalance skew splitting                                        |
| -------------------- | ---------------------------------------- | --------------------------------------------------------------- |
| Typical operator     | `SortMergeJoin(skew=true)`               | `Exchange ..., REBALANCE_PARTITIONS_*`                          |
| Main switch          | `spark.sql.adaptive.skewJoin.enabled`    | `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` |
| Best use             | Hot reducer in a join                    | Smoother write-side partition sizes                             |
| Verified plan marker | `AQEShuffleRead ... skewed` under a join | `AQEShuffleRead coalesced and skewed` after `REBALANCE`         |

______________________________________________________________________

## :material-alert-circle-outline: Behavior Gotchas

1. **`REBALANCE(n)` controls the initial exchange width, not a guaranteed final partition count**.
2. **AQE off means the hint is ignored** — it does not silently become ordinary `REPARTITION`.
3. **The adaptive work happens after the exchange is written** — Spark rewrites the read path, not the shuffle write itself.
4. **Coalescing and skew splitting can appear together** — the verified final marker was `AQEShuffleRead coalesced and skewed`.

______________________________________________________________________

## :material-brain: When to Use

| Scenario                                               | Recommendation                                              |
| ------------------------------------------------------ | ----------------------------------------------------------- |
| Final write partitions are uneven                      | Add `REBALANCE` near the write path                         |
| One rebalance partition is much larger than the others | Lower advisory size to encourage finer splits               |
| You only need join skew mitigation                     | Use [Optimizing Skew Join](optimizing-skew-join.md) instead |
