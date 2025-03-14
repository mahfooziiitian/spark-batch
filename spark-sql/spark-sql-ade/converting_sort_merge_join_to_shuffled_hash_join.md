# Converting sort merge join to shuffled hash join

AQE converts `sort-merge join` to `broadcast hash join` when the runtime statistics of any join side is smaller than the adaptive broadcast hash join threshold.

This is not as efficient as planning a broadcast hash join in the first place, but it's better than keep doing the sort-merge join, as we can save the sorting of both the join sides, and read shuffle files locally to save network traffic(if `spark.sql.adaptive.localShuffleReader.enabled` is true)

    spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold

## spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold

Configures the maximum size in bytes per partition that can be allowed to build local hash map.

If this value is not smaller than `spark.sql.adaptive.advisoryPartitionSizeInBytes` and all the partition sizes are not larger than this config, join selection prefers to use shuffled hash join instead of sort merge join regardless of the value of `spark.sql.join.preferSortMergeJoin`.
