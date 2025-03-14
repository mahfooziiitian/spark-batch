# How spark selects join strategy?

Taken directly from spark code.
let's see how spark decides on join strategy.

## If it is an '=' join

Look at the join hints, in the following order:

1. `Broadcast Hint`: Pick broadcast hash join if the join type is supported.
2. `Sort merge hint`: Pick sort-merge join if join keys are sortable.
3. `shuffle hash hint`: Pick shuffle hash join if the join type is supported.
4. `shuffle replicate NL hint`: pick cartesian product if join type is inner like.

If there is no hint or the hints are not applicable

1. `Pick broadcast hash join` if one side is small enough to broadcast, and the join type is supported.
2. `Pick shuffle hash join` if one side is small enough to build the `local hash map`, and is much smaller than the other side, and `spark.sql.join.preferSortMergeJoin` is false.
3. Pick `sort-merge join` if join keys are sortable.
4. Pick `cartesian product` if join type is inner .
5. Pick `broadcast nested loop join` as the final solution. It may OOM but there is no other choice.

## If it's not '=' join

Look at the join hints, in the following order:

1. `broadcast hint`: pick broadcast nested loop join.
2. `shuffle replicate NL hint`: pick cartesian product if join type is inner like.

If there is no hint or the hints are not applicable

1. Pick `broadcast nested loop join` if one side is small enough to broadcast.
2. Pick `cartesian product` if join type is inner like.
3. Pick `broadcast nested loop join` as the final solution. It may OOM but we don’t have any other choice.
