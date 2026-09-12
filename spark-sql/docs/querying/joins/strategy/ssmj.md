# :material-compare-horizontal: "Shuffle Sort-Merge Join" Terminology

Some materials say "shuffle sort-merge join" to emphasize that a sort-merge join normally requires a shuffle stage first. In Spark 4.2 plans, however, the operator name is simply `SortMergeJoin`.

### :material-animation-play: Interactive Visualization — Sort-Merge Terminology Map

<div id="viz-joins-strategy-ssmj" class="ts-viz"></div>

This terminology map shows what is new here and what overlaps with the main sort-merge page.

<script src="../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified in PySpark 4.2

A tested `MERGE`-hinted equi-join produced:

```text
SortMergeJoin [k#20L], [k#21L], Inner
```

Not `ShuffleSortMergeJoin`.

______________________________________________________________________

## :material-information-outline: How to Read the Name

| Phrase                  | Spark 4.2 meaning                                    |
| ----------------------- | ---------------------------------------------------- |
| Sort-merge join         | The physical operator name                           |
| Shuffle sort-merge join | Informal description of the full distributed process |
| SSMJ                    | Informal abbreviation, not a distinct plan node      |

So the overlap with `smj.md` is real: both pages discuss the same physical operator. This page should stay focused on terminology rather than re-explaining the whole algorithm.

______________________________________________________________________

## :material-code-tags: Why the "shuffle" Word Appears

A typical distributed `SortMergeJoin` plan contains:

```text
Exchange hashpartitioning(...)
Sort [... ASC NULLS FIRST]
SortMergeJoin [...], [...], Inner
```

That is why many practitioners casually say "shuffle sort-merge join," even though the operator label remains `SortMergeJoin`.

______________________________________________________________________

## :material-alert-outline: Scope Boundary

!!! note "Duplicate-content flag"

    This page and `smj.md` cover the same Spark 4.2 operator. The overlap is factual, not a new strategy. Treat this page as terminology clarification rather than a separate join type.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Read this page if you encounter SSMJ in older notes or interviews and want to map that term back to the exact Spark 4.2 physical operator name.
