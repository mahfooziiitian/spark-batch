# :material-transit-connection-variant: Query Lifecycle

This page follows a planned query into runtime: how physical operators become Spark jobs, stage boundaries, task sets, and observable execution artifacts.

### :material-animation-play: Interactive Visualization — Exchange Boundaries Become Stages

<div id="viz-execution-stage-boundaries" class="ts-viz"></div>

The visualization ties real plan shapes to runtime structure: no `Exchange` means a single stage, while a shuffle-producing `Exchange` splits the work into upstream and downstream task sets.

______________________________________________________________________

## :material-sitemap: Compilation and runtime are related, but not identical

Spark still goes through parse → analyze → optimize → physical plan → codegen, but those compiler phases are already documented in the planner internals pages. This page picks up at the moment the physical plan is ready to run.

| Layer               | Main question                                           | Best place to inspect it                            |
| ------------------- | ------------------------------------------------------- | --------------------------------------------------- |
| Logical compilation | Did Spark understand and rewrite my query correctly?    | `EXPLAIN EXTENDED`                                  |
| Physical planning   | Which join or aggregate strategy did Spark choose?      | `EXPLAIN`, `EXPLAIN FORMATTED`                      |
| Runtime scheduling  | How many jobs, stages, and tasks did the action launch? | Spark UI, `statusTracker()`, executed physical plan |

!!! note "See also"

    For parser/analyzer/optimizer detail, use [Query Parsing & Execution](../../internals/planner/query-parsing.md) and [Query Planner](../../internals/planner/query-planner.md). This page focuses on the runtime half of the story.

______________________________________________________________________

## :material-information-outline: Verified mapping — action to job, stage, and task

With AQE disabled and `local[2]`, this verified query:

```sql
SELECT
    id % 5 AS g,
    COUNT(*) AS c
FROM range(1000)
GROUP BY id % 5;
```

produced this physical-plan core in Spark 4.2:

```text
*(2) HashAggregate(keys=[_groupingexpression#...], functions=[count(1)])
+- Exchange hashpartitioning(_groupingexpression#..., 200), ENSURE_REQUIREMENTS
   +- *(1) HashAggregate(keys=[_groupingexpression#...], functions=[partial_count(1)])
      +- *(1) Project [(id#... % 5) AS _groupingexpression#...]
         +- *(1) Range (0, 1000, step=1, splits=2)
```

Using `spark.sparkContext.statusTracker()` after `collect()` showed:

| Verified signal       | Result                     |
| --------------------- | -------------------------- |
| Input partitions      | `2` (`Range ... splits=2`) |
| `Exchange` boundaries | `1`                        |
| Job count             | `1`                        |
| Stage IDs             | `[0, 1]`                   |
| Tasks in stage 0      | `2`                        |
| Tasks in stage 1      | `200`                      |

Why `200` tasks in the second stage? Because the shuffle stage honored the default `spark.sql.shuffle.partitions = 200`.

That gives you a practical translation:

- **One action** triggered the work.
- **One `Exchange`** created a stage boundary.
- **Each stage** ran a set of parallel tasks.
- **Task count** came from partitioning: input splits upstream, shuffle partitions downstream.

______________________________________________________________________

## :material-swap-horizontal: How to read runtime boundaries from a plan

| Plan marker                      | What it usually means at runtime                                                   |
| -------------------------------- | ---------------------------------------------------------------------------------- |
| `Exchange hashpartitioning(...)` | A shuffle boundary and therefore a new downstream stage                            |
| `BroadcastExchange ...`          | A broadcast build side that must be materialized before the consuming join can run |
| `*(1)`, `*(2)`                   | Whole-stage codegen regions, not task counts                                       |
| `AdaptiveSparkPlan`              | AQE may rewrite the initial physical plan during execution                         |
| `AQEShuffleRead coalesced`       | AQE merged small shuffle partitions after map-side statistics arrived              |

A good rule of thumb is to count `Exchange` operators first, then confirm in the Spark UI or with `statusTracker()`.

______________________________________________________________________

## :material-map-legend: What AQE changes

With AQE enabled, the same logical query may finish with a different executed plan than the initial one. A verified Spark 4.2 run of a grouped query showed:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage 1
   +- *(2) HashAggregate(...)
      +- AQEShuffleRead coalesced
         +- ShuffleQueryStage 0
            +- Exchange hashpartitioning(...)
```

Two important consequences follow:

1. The stable concept is still the **shuffle boundary** (`Exchange`).
2. Exact job counts can vary because AQE may materialize query stages and auxiliary jobs while re-optimizing.

So avoid overly rigid claims such as "every SQL query always becomes exactly one job." The safer, verified statement is that **actions launch jobs, and shuffle boundaries split the work into stages**.

______________________________________________________________________

## :material-magnify: Where to observe the lifecycle

Use these tools together:

| Tool                                 | Best for                                                           |
| ------------------------------------ | ------------------------------------------------------------------ |
| `EXPLAIN FORMATTED`                  | Mapping operators, exchanges, and codegen regions before execution |
| Spark UI → SQL tab                   | End-to-end SQL execution, duration, and generated DAG              |
| Spark UI → Jobs / Stages tabs        | Stage counts, task counts, skew, shuffle reads/writes              |
| `spark.sparkContext.statusTracker()` | Lightweight programmatic job and stage inspection                  |
| Driver logs                          | Failures, retries, Python exceptions, broadcast timeout clues      |

If the UI shows a slow stage, return to the physical plan and look for the matching `Exchange`, `SortMergeJoin`, `HashAggregate`, or scan node.

______________________________________________________________________

## :material-play-circle: Run

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local[2]")
         .config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

tracker = spark.sparkContext.statusTracker()
spark.sparkContext.setJobGroup("lifecycle-demo", "lifecycle-demo")

df = spark.sql("SELECT id % 5 AS g, COUNT(*) AS c FROM range(1000) GROUP BY id % 5")
df.collect()

for job_id in tracker.getJobIdsForGroup("lifecycle-demo"):
    info = tracker.getJobInfo(job_id)
    print(job_id, list(info.stageIds))

spark.stop()
```
