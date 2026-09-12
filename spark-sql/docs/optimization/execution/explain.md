# :material-magnify-expand: EXPLAIN Plans

`EXPLAIN` is the fastest way to verify how Spark 4.2 plans a query: which operators appear, where shuffles happen, whether statistics are available, and whether whole-stage code generation fused the plan.

### :material-animation-play: Interactive Visualization — Compare EXPLAIN Modes

<div id="viz-explain-mode-comparer" class="ts-viz"></div>

This visualization lets you switch among the five day-to-day `EXPLAIN` experiences for the same query and see what each one adds beyond the previous mode.

______________________________________________________________________

## :material-code-tags: Verified syntax

```sql
EXPLAIN statement;
EXPLAIN EXTENDED statement;
EXPLAIN FORMATTED statement;
EXPLAIN COST statement;
EXPLAIN CODEGEN statement;
```

These are the SQL forms verified against Spark 4.2.

| What you type                | First line returned by Spark 4.2        | Main use                             |
| ---------------------------- | --------------------------------------- | ------------------------------------ |
| `EXPLAIN SELECT 1`           | `== Physical Plan ==`                   | Quick physical-plan check            |
| `EXPLAIN EXTENDED SELECT 1`  | `== Parsed Logical Plan ==`             | Full logical-to-physical trace       |
| `EXPLAIN FORMATTED SELECT 1` | `== Physical Plan ==`                   | Readable numbered operator tree      |
| `EXPLAIN COST SELECT 1`      | `== Optimized Logical Plan ==`          | Statistics visibility                |
| `EXPLAIN CODEGEN SELECT 1`   | `Found ... WholeStageCodegen subtrees.` | Generated Java and fusion boundaries |

!!! note "`EXPLAIN SIMPLE` is not valid SQL syntax"

    Spark 4.2 rejected `EXPLAIN SIMPLE SELECT 1` with `ParseException`. `simple` is a `DataFrame.explain("simple")` mode name, not a SQL keyword.

______________________________________________________________________

## :material-information-outline: Verified behavior

1. `EXPLAIN` plans a query but does **not** execute it.
2. Bare `EXPLAIN` shows the physical plan only.
3. `EXPLAIN EXTENDED` emits Parsed → Analyzed → Optimized → Physical sections.
4. `EXPLAIN FORMATTED` prints a numbered operator tree followed by per-node details.
5. `EXPLAIN COST` may still show `sizeInBytes` before statistics are analyzed, especially for file-backed tables; what usually appears later is `rowCount`.
6. `EXPLAIN CODEGEN` can legitimately show `Found 0 WholeStageCodegen subtrees.` for some adaptive or non-fuseable plans.

______________________________________________________________________

## :material-flask-outline: Verified output excerpts

### Bare `EXPLAIN`

For:

```sql
EXPLAIN
SELECT id % 2 AS g, COUNT(*) AS c
FROM range(10)
GROUP BY id % 2;
```

Spark 4.2 returned a physical plan headed by:

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[_groupingexpression#...], functions=[count(1)])
   +- Exchange hashpartitioning(_groupingexpression#..., 200), ENSURE_REQUIREMENTS
      +- HashAggregate(keys=[_groupingexpression#...], functions=[partial_count(1)])
         +- Project [(id#... % 2) AS _groupingexpression#...]
            +- Range (0, 10, step=1, splits=2)
```

Use this mode when you only need the chosen runtime operators.

### `EXPLAIN EXTENDED`

The same query under `EXPLAIN EXTENDED` began with:

```text
== Parsed Logical Plan ==
'Aggregate [('id % 2)], [('id % 2) AS g#..., 'count(1) AS c#...]
+- 'UnresolvedTableValuedFunction [range], [10], false

== Analyzed Logical Plan ==
g: bigint, c: bigint
Aggregate [(id#... % cast(2 as bigint))], ...

== Optimized Logical Plan ==
Aggregate [_groupingexpression#...], ...

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
```

This is the best mode when you need to see unresolved names become typed expressions and then optimized operators.

### `EXPLAIN FORMATTED`

`EXPLAIN FORMATTED` on the same query produced a numbered tree:

```text
== Physical Plan ==
AdaptiveSparkPlan (6)
+- HashAggregate (5)
   +- Exchange (4)
      +- HashAggregate (3)
         +- Project (2)
            +- Range (1)
```

followed by per-node blocks such as:

```text
(3) HashAggregate
Input [1]: [_groupingexpression#...]
Keys [1]: [_groupingexpression#...]
Functions [1]: [partial_count(1)]
```

This is usually the most readable format for multi-join or multi-aggregate queries.

### `EXPLAIN COST` before and after statistics

A verified Parquet table example showed three distinct states.

Before `ANALYZE TABLE`:

```text
== Optimized Logical Plan ==
Aggregate [grp#...], [grp#..., count(1) AS count(1)#...], Statistics(sizeInBytes=2043.0 B)
+- Project [grp#...], Statistics(sizeInBytes=1362.0 B)
   +- Relation spark_catalog.default.stat_check_tbl[...] parquet, Statistics(sizeInBytes=2044.0 B)
```

After `ANALYZE TABLE stat_check_tbl COMPUTE STATISTICS`:

```text
== Optimized Logical Plan ==
Aggregate [grp#...], [grp#..., count(1) AS count(1)#...], Statistics(sizeInBytes=2.3 KiB)
+- Project [grp#...], Statistics(sizeInBytes=1600.0 B, rowCount=100)
   +- Relation spark_catalog.default.stat_check_tbl[...] parquet, Statistics(sizeInBytes=2.3 KiB, rowCount=100)
```

After `ANALYZE TABLE stat_check_tbl COMPUTE STATISTICS FOR ALL COLUMNS`:

```text
== Optimized Logical Plan ==
Aggregate [grp#...], [grp#..., count(1) AS count(1)#...], Statistics(sizeInBytes=240.0 B, rowCount=10)
+- Project [grp#...], Statistics(sizeInBytes=1600.0 B, rowCount=100)
   +- Relation spark_catalog.default.stat_check_tbl[...] parquet, Statistics(sizeInBytes=2.3 KiB, rowCount=100)
```

The correction is important: Spark 4.2 did **not** show `?` for the un-analyzed table. It still knew file-size estimates, but row-count information improved only after statistics collection.

### `EXPLAIN CODEGEN`

For a fuseable query:

```sql
EXPLAIN CODEGEN
SELECT id + 1 AS v
FROM range(5)
WHERE id > 1;
```

Spark 4.2 returned:

```text
Found 1 WholeStageCodegen subtrees.
== Subtree 1 / 1 (maxMethodCodeSize:312; maxConstantPoolSize:196(0.30% used); numInnerClasses:0) ==
*(1) Project [(id#... + 1) AS v#...]
+- *(1) Filter (id#... > 1)
   +- *(1) Range (0, 5, step=1, splits=2)

Generated code:
/* 001 */ public Object generate(Object[] references) {
```

For the earlier adaptive aggregate query, `EXPLAIN CODEGEN` returned `Found 0 WholeStageCodegen subtrees.` So treat this mode as a fusion diagnostic, not a guarantee of generated code for every query shape.

______________________________________________________________________

## :material-swap-horizontal: Reading the physical-plan markers

| Marker              | What to look for                                        |
| ------------------- | ------------------------------------------------------- |
| `Exchange`          | Shuffle boundary; likely a new stage                    |
| `BroadcastExchange` | Small-side materialization for a broadcast join         |
| `PushedFilters`     | Evidence that filter predicates reached the data source |
| `ReadSchema`        | Evidence of column pruning                              |
| `*(N)`              | Whole-stage codegen region                              |
| `AdaptiveSparkPlan` | AQE wrapper; initial and final plans may differ         |

If the plan is large, start with `FORMATTED`; if you need rewrite history, step back to `EXTENDED`.

______________________________________________________________________

## :material-play-circle: Run

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sql = "SELECT id % 2 AS g, COUNT(*) AS c FROM range(10) GROUP BY id % 2"
for mode in [None, "extended", "formatted", "cost", "codegen"]:
    print(f"\n--- mode={mode or 'plain'} ---")
    if mode is None:
        spark.sql(f"EXPLAIN {sql}").show(truncate=False)
    else:
        spark.sql(f"EXPLAIN {mode.upper()} {sql}").show(truncate=False)

spark.stop()
```
