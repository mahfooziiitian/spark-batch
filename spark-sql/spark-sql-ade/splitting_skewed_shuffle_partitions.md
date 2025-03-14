# Spliting skewed shuffle partitions

## spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled

When true and spark.sql.adaptive.enabled is true, Spark will optimize the skewed shuffle partitions in RebalancePartitions and split them to smaller ones according to the target size (specified by `spark.sql.adaptive.advisoryPartitionSizeInBytes`), to avoid data skew.

## spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor

A partition will be merged during splitting if its size is small than this factor multiply `spark.sql.adaptive.advisoryPartitionSizeInBytes`.
