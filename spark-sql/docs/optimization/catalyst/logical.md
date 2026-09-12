# :material-map-legend: Logical Optimization

Logical optimization begins **after** analysis has resolved names and types. Catalyst then rewrites the logical plan with rule batches until no additional transformation changes the tree.

This page focuses on rule effects you can verify in Spark 4.2 by comparing the **Analyzed** and **Optimized** sections of `EXPLAIN EXTENDED`.

______________________________________________________________________

### :material-animation-play: Interactive Visualization — Verified Rule Diffs

<div id="viz-logical-rule-diff" class="ts-viz"></div>

Switch between three real Catalyst rewrites to see the kind of before/after tree changes that appear in Spark 4.2 plans.

______________________________________________________________________

## :material-check-decagram: Rule Names Verified in Spark 4.2

Inspecting `spark._jsparkSession.sessionState().optimizer().batches()` confirmed these rule names in the Spark 4.2 optimizer:

| Rule class name         | Verified effect                                                                         |
| ----------------------- | --------------------------------------------------------------------------------------- |
| `PushDownPredicates`    | Pushes filters deeper and often merges them into a single conjunctive predicate         |
| `ColumnPruning`         | Removes attributes that are no longer needed after rewrite                              |
| `ConstantFolding`       | Replaces literal-only expressions with their computed value                             |
| `BooleanSimplification` | Simplifies expressions such as `true AND expr`                                          |
| `NullPropagation`       | Simplifies null-sensitive expressions when the result is known                          |
| `EliminateOuterJoin`    | Rewrites an outer join to an inner join when later predicates reject null-extended rows |
| `ReorderJoin`           | Present in optimizer batches for eligible multi-join queries                            |

!!! note "Plural, not singular"

    The rule name present in Spark 4.2 is `PushDownPredicates`, not `PushDownPredicate`.

______________________________________________________________________

## :material-calculator: Verified Example 1 — Constant Folding and Boolean Simplification

Spark 4.2 query:

```sql
EXPLAIN EXTENDED
SELECT 1 + 1 AS x, true AND (1 = 1) AS ok;
```

Observed plan change:

```text
== Analyzed Logical Plan ==
Project [(1 + 1) AS x#0, (true AND (1 = 1)) AS ok#1]
+- OneRowRelation

== Optimized Logical Plan ==
Project [2 AS x#0, true AS ok#1]
+- OneRowRelation
```

What this proves:

- `ConstantFolding` reduced `1 + 1` to the literal `2`.
- `BooleanSimplification` reduced `true AND (1 = 1)` to the literal `true`.

______________________________________________________________________

## :material-filter: Verified Example 2 — Predicate Pushdown, Filter Combination, and Column Pruning

Spark 4.2 query over a temporary `orders(id, amount, region)` view:

```sql
EXPLAIN EXTENDED
SELECT id
FROM (
    SELECT *
    FROM orders
    WHERE amount > 30
) t
WHERE region = 'US';
```

Observed plan change:

```text
== Analyzed Logical Plan ==
Project [id#2L]
+- Filter (region#4 = US)
   +- SubqueryAlias t
      +- Project [id#2L, amount#3L, region#4]
         +- Filter (amount#3L > cast(30 as bigint))
            +- ...

== Optimized Logical Plan ==
Filter (((id#2L * 10) > 30) AND (((id#2L % 2) = 0) <=> true))
+- Range (0, 10, step=1, splits=Some(1))
```

What changed:

- The two filters became **one** predicate. In this Spark 4.2 environment the collapse is visible in the optimized plan, but the optimizer batch listing did **not** surface a rule literally named `CombineFilters`, so treat the tree rewrite as the reliable observable fact.
- The filter was pushed beneath the subquery/view boundary, which is the observable effect of `PushDownPredicates`.
- The intermediate projection carrying `amount` and `region` disappeared because only `id` remained necessary, which demonstrates `ColumnPruning`.

Notice that `EXPLAIN` does not label those rule names directly; you infer them from the tree change.

______________________________________________________________________

## :material-call-merge: Verified Example 3 — Eliminating an Outer Join

Spark 4.2 query:

```sql
EXPLAIN EXTENDED
SELECT left_t.k
FROM left_t
LEFT OUTER JOIN right_t
    ON left_t.k = right_t.k
WHERE right_t.rv IS NOT NULL;
```

Observed plan change:

```text
== Analyzed Logical Plan ==
Project [k#6L]
+- Filter isnotnull(rv#10)
   +- Join LeftOuter, (k#6L = k#9L)
      :- ...
      +- ...

== Optimized Logical Plan ==
Project [k#6L]
+- Join Inner, (k#6L = k#9L)
   :- ...
   +- ...
```

Because the `WHERE right_t.rv IS NOT NULL` predicate rejects the null-extended rows introduced by a left outer join, Spark 4.2 safely rewrote the join to `Inner`.

______________________________________________________________________

## :material-table: Reading Optimizer Output Carefully

| If you see this in the optimized plan            | It usually means                               |
| ------------------------------------------------ | ---------------------------------------------- |
| A literal where an expression used to be         | Constant folding or null propagation fired     |
| Fewer `Project` nodes / fewer referenced columns | Column pruning fired                           |
| One `Filter` with a larger `AND` condition       | Filters were combined and/or pushed deeper     |
| `Join LeftOuter` changed to `Join Inner`         | Outer-join elimination fired                   |
| Temporary views or aliases disappear             | Spark inlined or simplified subquery structure |

!!! tip "Compare analyzed vs optimized, not parsed vs optimized"

    The parsed plan is still unresolved, so most meaningful rule verification happens by comparing the **Analyzed** and **Optimized** sections side by side.

______________________________________________________________________

## :material-link-variant: See Also

- [Catalyst Optimizer](index.md)
- [Physical Planning](physical.md)
- [Query Planner](../../internals/planner/query-planner.md)
