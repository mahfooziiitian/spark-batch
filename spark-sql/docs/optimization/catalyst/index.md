# :material-atom: Catalyst Optimizer

Catalyst is Spark SQL's query compiler: it turns SQL text into an unresolved logical plan, resolves it with the Analyzer, rewrites it through optimizer batches, chooses physical operators, and finally emits codegen-friendly execution stages.

This section stays deliberately complementary to the broader pipeline overview in [Query Parsing & Execution](../../internals/planner/query-parsing.md) and the strategy catalogue in [Query Planner](../../internals/planner/query-planner.md): here the emphasis is on **Catalyst-specific rule mechanics** and what you can actually verify with `EXPLAIN` in Spark 4.2.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — Catalyst Fixed-Point Loop

<div id="viz-catalyst-fixed-point" class="ts-viz"></div>

The optimizer does not apply one giant rewrite. It runs batches of rules repeatedly until the plan stops changing, then passes the optimized tree to physical planning and whole-stage code generation.

______________________________________________________________________

## :material-sitemap: Verified Pipeline Stages

| Stage             | Input                        | Output                             | What you can observe directly                               |
| ----------------- | ---------------------------- | ---------------------------------- | ----------------------------------------------------------- |
| Parsing           | SQL text                     | Unresolved logical plan            | `== Parsed Logical Plan ==` in `EXPLAIN EXTENDED`           |
| Analysis          | Unresolved plan              | Resolved logical plan              | `== Analyzed Logical Plan ==`                               |
| Optimization      | Resolved plan                | Optimized logical plan             | `== Optimized Logical Plan ==`                              |
| Physical planning | Optimized plan               | Selected `SparkPlan`               | `== Physical Plan ==`, best viewed with `EXPLAIN FORMATTED` |
| Code generation   | Compatible physical subtrees | Generated Java source for subtrees | `EXPLAIN CODEGEN`                                           |

A useful correction to older Catalyst summaries: Spark SQL does **not** expose the raw ANTLR parse tree through `EXPLAIN`. The first observable artifact is already the unresolved logical plan built by `AstBuilder`. See [AST](ast/spark-sql-ast.md) for details.

______________________________________________________________________

## :material-flask-outline: Verified `EXPLAIN EXTENDED` Milestones

The following Spark 4.2 query is small enough to read end-to-end but still shows all major transitions:

```sql
EXPLAIN EXTENDED
SELECT k + 1 AS v
FROM big_t
WHERE k < 3;
```

Spark 4.2 produced these stage changes:

```text
== Parsed Logical Plan ==
'Project [('k + 1) AS v#12]
+- 'Filter ('k < 3)
   +- 'UnresolvedRelation [big_t], [], false

== Analyzed Logical Plan ==
Project [(k#1L + cast(1 as bigint)) AS v#12L]
+- Filter (k#1L < cast(3 as bigint))
   +- SubqueryAlias big_t
      +- View (`big_t`, [k#1L, v1#2L])
         +- ...

== Optimized Logical Plan ==
Project [(id#0L + 1) AS v#12L]
+- Filter (id#0L < 3)
   +- Range (0, 1000, step=1, splits=Some(1))

== Physical Plan ==
*(1) Project [(id#0L + 1) AS v#12L]
+- *(1) Filter (id#0L < 3)
   +- *(1) Range (0, 1000, step=1, splits=1)
```

What changed between stages:

- **Parsed**: unresolved names are printed with a leading apostrophe (`'k`, `'Project`, `'UnresolvedRelation`).
- **Analyzed**: Spark resolves names, adds expression IDs, and inserts type casts such as `cast(1 as bigint)`.
- **Optimized**: the temporary view is inlined and redundant casts are simplified away.
- **Physical**: the `*(1)` markers show the operators were fused into a single whole-stage-codegen subtree.

______________________________________________________________________

## :material-cog-refresh: What the Optimizer Actually Runs

Spark 4.2 exposes optimizer batches through the JVM session state. Inspecting
`sessionState().optimizer().batches()` confirms that rules such as
`PushDownPredicates`, `ColumnPruning`, `ConstantFolding`, `BooleanSimplification`,
`NullPropagation`, and `EliminateOuterJoin` appear in repeated batches including:

- `Operator Optimization before Inferring Filters`
- `Operator Optimization after Inferring Filters`

That fixed-point structure matters: a rule can enable another rule, so Catalyst keeps iterating until the tree stabilizes.

!!! note "`EXPLAIN` shows results, not rule names"

    `EXPLAIN EXTENDED` proves that a rewrite happened by comparing the analyzed and optimized plans, but it does **not** annotate which rule fired. To see exact rule class names you must inspect the optimizer from the JVM side, as done above.

______________________________________________________________________

## :material-format-list-bulleted: What This Subsection Covers

| Page                                  | Focus                                                                                                              |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| [Logical Optimization](logical.md)    | Verified rule effects such as constant folding, predicate pushdown, filter combination, and outer-join elimination |
| [Physical Planning](physical.md)      | Observable join and aggregation strategy selection in `EXPLAIN FORMATTED`                                          |
| [Code Generation](code-generation.md) | Whole-stage-codegen markers and generated Java emitted by `EXPLAIN CODEGEN`                                        |
| [AST](ast/spark-sql-ast.md)           | What Spark exposes before analysis, and what remains internal to the parser                                        |

______________________________________________________________________

## :material-link-variant: See Also

- [Query Parsing & Execution](../../internals/planner/query-parsing.md)
- [Query Planner](../../internals/planner/query-planner.md)
