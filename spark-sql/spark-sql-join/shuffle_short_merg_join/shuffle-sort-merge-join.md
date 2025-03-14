# Shuffle sort-merge join

It involves

1. shuffling of data to get the same join_key with the same worker.
2. Then performing `sort-merge join` operation at the partition level in the worker nodes.

## steps

1. `Shuffle Phase` – both datasets are shuffled
2. `Sort Phase` – records are sorted by key on both sides
3. `Merge Phase` – iterate over both sides and join based on the join key.

## Note

1. Since spark 2.3, this is the default join strategy in spark and can be disabled with

    set spark.sql.join.preferSortMergeJoin

2. Only supported for `'='` join.
3. The join keys need to be `sortable`(obviously).
4. Supported for `all join types`.
