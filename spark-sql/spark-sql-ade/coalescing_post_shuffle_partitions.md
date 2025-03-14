# Coalescing Post Shuffle Partitions

This feature coalesces the post shuffle partitions based on the map output statistics when both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled` configurations are true.

This feature simplifies the `tuning of shuffle partition` number when running queries.

You do not need to set a proper shuffle partition number to fit your dataset.

Spark can pick the proper shuffle partition number at runtime once you set a large enough initial number of shuffle partitions.

 spark.sql.adaptive.coalescePartitions.initialPartitionNum

## spark.sql.adaptive.coalescePartitions.enabled

When true and spark.sql.adaptive.enabled is true, Spark will coalesce contiguous shuffle partitions according to the target size (specified by `spark.sql.adaptive.advisoryPartitionSizeInBytes`), to avoid too many small tasks.

## spark.sql.adaptive.coalescePartitions.parallelismFirst

When true, Spark ignores the target size specified by `spark.sql.adaptive.advisoryPartitionSizeInBytes` (default 64MB) when coalescing contiguous shuffle partitions, and only respect the minimum partition size specified by `spark.sql.adaptive.coalescePartitions.minPartitionSize` (default 1MB), to maximize the parallelism.

This is to avoid performance regression when enabling adaptive query execution.

It's recommended to set this config to false and respect the target size specified by `spark.sql.adaptive.advisoryPartitionSizeInBytes`.

## spark.sql.adaptive.coalescePartitions.minPartitionSize

The minimum size of shuffle partitions after coalescing. Its value can be at most 20% of spark.sql.adaptive.advisoryPartitionSizeInBytes. This is useful when the target size is ignored during partition coalescing, which is the default case.

## spark.sql.adaptive.coalescePartitions.initialPartitionNum

The initial number of shuffle partitions before coalescing. If not set, it equals to spark.sql.shuffle.partitions. This configuration only has an effect when spark.sql.adaptive.enabled and spark.sql.adaptive.coalescePartitions.enabled are both enabled.

## spark.sql.adaptive.advisoryPartitionSizeInBytes

The advisory size in bytes of the shuffle partition during adaptive optimization (when spark.sql.adaptive.enabled is true). It takes effect when Spark coalesces small shuffle partitions or splits skewed shuffle partition.
