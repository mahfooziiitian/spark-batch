# :material-code-braces: Code Generation

Whole-stage code generation fuses compatible physical operators into a small number of generated Java subtrees. Spark 4.2 makes that fusion directly observable with both `EXPLAIN FORMATTED` and `EXPLAIN CODEGEN`.

This page focuses on what the engine actually prints, rather than on generic Tungsten lore.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — Whole-Stage Fusion

<div id="viz-codegen-fusion" class="ts-viz"></div>

A whole-stage-codegen subtree is not "the entire query". It is a compatible slice of the physical plan that Spark can fuse into one generated iterator class.

______________________________________________________________________

## :material-eye: Verified Example — `EXPLAIN FORMATTED`

Spark 4.2 query:

```sql
EXPLAIN FORMATTED
SELECT k, v1 + 1 AS v
FROM big_t
WHERE k < 3;
```

Observed output:

```text
== Physical Plan ==
* Project (3)
+- * Filter (2)
   +- * Range (1)

(1) Range [codegen id : 1]
Output [1]: [id#0L]

(2) Filter [codegen id : 1]
Condition : (id#0L < 3)

(3) Project [codegen id : 1]
Output [2]: [id#0L AS k#1L, (id#0L + 1) AS v#10L]
```

Two important verified signals appear here:

- the `*` marker shows the operator belongs to a codegen subtree,
- the shared `codegen id : 1` proves `Range`, `Filter`, and `Project` were fused together.

______________________________________________________________________

## :material-file-code-outline: Verified Example — `EXPLAIN CODEGEN`

Running the same query with `EXPLAIN CODEGEN` produced this header in Spark 4.2:

```text
Found 1 WholeStageCodegen subtrees.
== Subtree 1 / 1 ==
*(1) Project [id#0L AS k#1L, (id#0L + 1) AS v#11L]
+- *(1) Filter (id#0L < 3)
   +- *(1) Range (0, 1000, step=1, splits=1)
```

Then Spark printed generated Java source, including a stage-specific iterator class:

```java
public Object generate(Object[] references) {
  return new GeneratedIteratorForCodegenStage1(references);
}

final class GeneratedIteratorForCodegenStage1
    extends org.apache.spark.sql.execution.BufferedRowIterator {
  ...
  protected void processNext() throws java.io.IOException {
    ...
    boolean filter_value_0 = range_value_0 < 3L;
    if (!filter_value_0) continue;
    project_doConsume_0(range_value_0);
  }
}
```

This corrects a common overstatement: `EXPLAIN CODEGEN` shows the generated **Java source** for fused subtrees, not raw JVM bytecode.

______________________________________________________________________

## :material-transit-connection-variant: What Was Fused

In the verified query above, Spark 4.2 fused three operators into one subtree:

| Physical operator | Role in generated code                                       |
| ----------------- | ------------------------------------------------------------ |
| `Range`           | Produces input rows inside the generated loop                |
| `Filter`          | Becomes an `if` guard inside `processNext()`                 |
| `Project`         | Becomes row-writing logic such as `project_doConsume_0(...)` |

That is the practical meaning of whole-stage code generation: Spark removes row-by-row virtual handoff between these operators and emits one iterator that performs all three steps in one tight loop.

______________________________________________________________________

## :material-alert-outline: Interpreting Codegen Output Carefully

| Observation                          | Meaning                                                    |
| ------------------------------------ | ---------------------------------------------------------- |
| `Found N WholeStageCodegen subtrees` | Fusion happened, but only for compatible parts of the plan |
| Different `codegen id` values        | Operators belong to different fused subtrees               |
| No `*` marker on an operator         | That node sits outside the current codegen subtree         |
| `AdaptiveSparkPlan` around the tree  | AQE may still change the surrounding physical plan         |

!!! tip "Use both explain modes together"

    `EXPLAIN FORMATTED` is best for locating codegen boundaries in the physical tree. `EXPLAIN CODEGEN` is best for confirming what Java Spark actually emitted for each subtree.

______________________________________________________________________

## :material-link-variant: See Also

- [Physical Planning](physical.md)
- [AST](ast/spark-sql-ast.md)
- [Query Parsing & Execution](../../internals/planner/query-parsing.md)
