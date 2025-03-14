# Converting sort merge join to broadcast join

AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side is smaller than the adaptive broadcast hash join threshold.

This is not as efficient as planning a broadcast hash join in the first place, but it’s better than keep doing the sort-merge join, as we can save the sorting of both the join sides, and read shuffle files locally to save network traffic(if spark.sql.adaptive.localShuffleReader.enabled is true)

    spark.sql.adaptive.autoBroadcastJoinThreshold
    spark.sql.adaptive.localShuffleReader.enabled

## spark.sql.adaptive.autoBroadcastJoinThreshold

Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1, broadcasting can be disabled. The default value is the same as spark.sql.autoBroadcastJoinThreshold. Note that, this config is used only in adaptive framework.

## spark.sql.adaptive.localShuffleReader.enabled

When true and spark.sql.adaptive.enabled is true, Spark tries to use local shuffle reader to read the shuffle data when the shuffle partitioning is not needed, for example, after converting sort-merge join to broadcast-hash join.
