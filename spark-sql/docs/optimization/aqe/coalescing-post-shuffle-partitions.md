# :material-table-merge-cells: Coalescing Post-Shuffle Partitions

AQE partition coalescing is the simplest adaptive rewrite to verify in Spark 4.2:
a query starts with a wide shuffle, executes it, and the final plan replaces the raw
shuffle reader with `AQEShuffleRead coalesced`.

______________________________________________________________________

## :material-sitemap: Overview

Spark still creates the original shuffle map outputs, but AQE can read several adjacent
reduce partitions together when they are too small to justify separate tasks.

### :material-animation-play: Interactive Visualization — Before and After Coalescing

<div id="viz-aqe-coalesce" class="ts-viz"></div>

The bars show many tiny post-shuffle partitions being merged into fewer, larger reads
that better match the advisory partition size.

______________________________________________________________________

## :material-check-decagram: Verified Spark 4.2 Settings

| Property                                                    | Verified default | Why it matters                        |
| ----------------------------------------------------------- | ---------------- | ------------------------------------- |
| `spark.sql.adaptive.coalescePartitions.enabled`             | `true`           | Turns the feature on                  |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst`    | `true`           | Prefers keeping more tasks by default |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize`    | `1048576b`       | 1 MiB lower bound                     |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | `None`           | Unset unless you provide an override  |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes`           | `67108864b`      | 64 MiB target size                    |

______________________________________________________________________

## :material-flask-outline: Verified Example

PySpark 4.2.0 was run with:

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.shuffle.partitions = 40;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 1048576;
```

Query:

```sql
SELECT id % 5 AS g, COUNT(*) AS cnt
FROM range(1000)
GROUP BY id % 5;
```

Final plan after execution:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage
   +- HashAggregate(keys=[g], functions=[count(1)])
      +- AQEShuffleRead coalesced
         +- ShuffleQueryStage
            +- Exchange hashpartitioning(g, 40)
               +- HashAggregate(keys=[g], functions=[partial_count(1)])
```

Verified result rows were five groups of `count=200`, confirming that Spark kept the
semantics but shrank the read-side task layout.

______________________________________________________________________

## :material-compare: `parallelismFirst=true` vs `false`

| Setting | Practical effect in Spark 4.2                                                   |
| ------- | ------------------------------------------------------------------------------- |
| `true`  | Keeps more tasks even when they are smaller than the advisory size              |
| `false` | Honors the advisory size more aggressively, usually producing fewer write tasks |

!!! tip "Common ETL override"

    For write-heavy pipelines, `parallelismFirst=false` is often the more useful setting
    because it reduces tiny-file output without changing the upstream shuffle width.

______________________________________________________________________

## :material-alert-circle-outline: Behavior Gotchas

1. **Coalescing is read-side only** — the shuffle files are already written.
2. **It merges adjacent reducers, not arbitrary partitions** — AQE does not rebalance the shuffle globally.
3. **`initialPartitionNum=None` is a real default** — the docs should not present it as a literal alias value.
4. **Spark 4.2 plan output says `AQEShuffleRead coalesced`** — that is the clearest verification marker.

______________________________________________________________________

## :material-brain: When to Tune

| Symptom                                   | First knob                                                      |
| ----------------------------------------- | --------------------------------------------------------------- |
| Too many tiny output files                | `parallelismFirst=false`                                        |
| Still too many partitions after shuffle   | Increase initial shuffle width so AQE has more merge candidates |
| Large tasks after over-aggressive merging | Lower `advisoryPartitionSizeInBytes`                            |
