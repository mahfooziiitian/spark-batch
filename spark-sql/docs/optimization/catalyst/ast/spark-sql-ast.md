# :material-file-tree: Spark SQL AST and the Parsed Logical Plan

Spark SQL starts with an ANTLR grammar, but the SQL interfaces you use for debugging do **not** expose the raw parse tree. What `EXPLAIN EXTENDED` shows first is the unresolved logical plan that `AstBuilder` produced from that parse tree.

That distinction matters: the parsed-plan section is the closest observable proxy for the AST, but it is already one step past the raw ANTLR tree.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — From SQL Text to Parsed Plan

<div id="viz-ast-node-explorer" class="ts-viz"></div>

Use the stages to see where Spark stops exposing parser internals and starts exposing logical-plan nodes such as `'Project`, `'Filter`, and `'UnresolvedRelation`.

______________________________________________________________________

## :material-source-branch: Verified Observable Boundary

For Spark 4.2, this query:

```sql
EXPLAIN EXTENDED
SELECT k + 1 AS v
FROM big_t
WHERE k < 3;
```

printed the following first section:

```text
== Parsed Logical Plan ==
'Project [('k + 1) AS v#12]
+- 'Filter ('k < 3)
   +- 'UnresolvedRelation [big_t], [], false
```

The same explain then continued with analyzed and optimized sections:

```text
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
```

This verifies three important facts:

1. Spark exposes the unresolved logical plan with `'`-prefixed nodes.
2. The Analyzer, not the parser, inserts type casts and resolves attribute IDs.
3. The raw ANTLR parse tree itself is not printed by `EXPLAIN`.

______________________________________________________________________

## :material-table: What You Can and Cannot Observe

| Artifact                                      | Exposed directly from SQL? | Where to look                                     |
| --------------------------------------------- | -------------------------- | ------------------------------------------------- |
| Raw SQL text                                  | Yes                        | Your query string                                 |
| ANTLR parse tree                              | No                         | Internal parser only                              |
| `AstBuilder` output (unresolved logical plan) | Yes                        | `== Parsed Logical Plan ==` in `EXPLAIN EXTENDED` |
| Resolved logical plan                         | Yes                        | `== Analyzed Logical Plan ==`                     |
| Optimized logical plan                        | Yes                        | `== Optimized Logical Plan ==`                    |

So, when people say "Spark shows the AST," the precise statement is: Spark shows the **unresolved logical plan derived from the parser output**, not the concrete syntax tree produced directly by ANTLR.

______________________________________________________________________

## :material-format-list-bulleted-type: Common Parsed-Plan Nodes

| Parsed node               | Meaning before analysis                                              |
| ------------------------- | -------------------------------------------------------------------- |
| `'UnresolvedRelation [t]` | A table or view name that has not been bound to catalog metadata yet |
| `'k` / `'amount`          | Unresolved attribute references                                      |
| `'Project`                | The `SELECT` list as a logical operator                              |
| `'Filter`                 | The `WHERE` predicate before resolution                              |
| `'Aggregate`              | A grouping/aggregation operator before function resolution           |
| `'Join`                   | A join operator whose inputs and condition are still unresolved      |

The leading apostrophe is the quickest visual clue that you are still looking at a pre-analysis tree.

______________________________________________________________________

## :material-compass-outline: Practical Use

| If you are debugging...           | Check this section first                                           |
| --------------------------------- | ------------------------------------------------------------------ |
| Parser vs analyzer confusion      | If `== Parsed Logical Plan ==` exists, the SQL parsed successfully |
| Alias and column binding          | Compare parsed names with analyzed `#id`-suffixed attributes       |
| View expansion and simplification | Compare analyzed vs optimized, not just parsed vs analyzed         |

!!! note "No `EXPLAIN PARSED` mode"

    Spark 4.2 exposes the parsed stage only as part of `EXPLAIN EXTENDED`. There is no separate SQL-facing `EXPLAIN PARSED` command.

______________________________________________________________________

## :material-link-variant: See Also

- [Catalyst Optimizer](../index.md)
- [Logical Optimization](../logical.md)
- [Query Parsing & Execution](../../../internals/planner/query-parsing.md)
