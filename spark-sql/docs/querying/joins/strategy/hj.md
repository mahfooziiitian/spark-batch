# :material-pound-box: Hash Join as a Family

In Spark documentation and discussion, "hash join" often means the build-and-probe algorithm family, not a standalone physical operator named `HashJoin`.

### :material-animation-play: Interactive Visualization — Hash Join Family

<div id="viz-joins-strategy-hash-family" class="ts-viz"></div>

Compare the two concrete Spark 4.2 hash-join operators and see why the generic term is useful but incomplete.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: What PySpark 4.2 Actually Shows

In the verification queries for this section, Spark 4.2 produced:

- `BroadcastHashJoin` for a small broadcastable equi-join.
- `ShuffledHashJoin` for an equi-join with an explicit `SHUFFLE_HASH` hint.
- No plan node named just `HashJoin`.

So this page is about the algorithmic family, while the specific operator pages cover the actual plan nodes.

______________________________________________________________________

## :material-source-branch: Build-Probe Algorithm

Every hash join has the same core idea:

1. Pick a build side.
2. Materialize a hash table keyed by the join columns.
3. Stream the other side and probe the hash table for matches.

The difference in Spark is how matching rows get co-located before that build-probe work happens.

| Variant        | How rows are co-located                                          | Physical operator   |
| -------------- | ---------------------------------------------------------------- | ------------------- |
| Broadcast hash | Small side is broadcast to every executor                        | `BroadcastHashJoin` |
| Shuffle hash   | Both sides shuffle by key, then one side is hashed per partition | `ShuffledHashJoin`  |

______________________________________________________________________

## :material-information-outline: What Hash Joins Need

Hash joins in Spark 4.2 require:

- Equality-based join keys.
- A join type supported by the chosen concrete operator.
- Enough memory for the chosen build side.

They do **not** require sortable keys, which is one reason `ShuffledHashJoin` exists beside `SortMergeJoin`.

______________________________________________________________________

## :material-code-tags: Reading the Plan

```text
BroadcastHashJoin [k#0L], [k#1L], Inner, BuildRight
ShuffledHashJoin [k#12L], [k#13L], Inner, BuildRight
```

The operator name tells you which physical variant Spark chose; `BuildRight` tells you which side became the hash table.

______________________________________________________________________

## :material-alert-outline: Avoid Page Overlap Confusion

!!! note "Generic term vs operator name"

    `bhj.md` and `shj.md` document concrete physical operators. This page explains the shared algorithm behind them. They overlap by design, but they are not duplicates if you keep that distinction clear.

______________________________________________________________________

## :material-lightbulb-outline: When to Use This Page

Use this page when you want the mental model for hash-based joins as a category. Use the operator-specific pages when you need exact Spark 4.2 trigger conditions.
