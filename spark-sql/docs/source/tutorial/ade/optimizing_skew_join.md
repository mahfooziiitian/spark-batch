# Data skew

Data skew can severely downgrade the performance of join queries.

This feature dynamically handles skew in `sort-merge join` by splitting (and replicating if needed) skewed tasks into roughly evenly sized tasks.
It takes effect when both `spark.sql.adaptive.enabled` and `spark.sql.adaptive.skewJoin.enabled` configurations are enabled.

## spark.sql.adaptive.skewJoin.enabled

When true and spark.sql.adaptive.enabled is true, Spark dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed partitions.

## spark.sql.adaptive.skewJoin.skewedPartitionFactor

A partition is considered as skewed if its size is larger than this factor multiplying the median partition size and also larger than spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes.

## spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes

A partition is considered as skewed if its size in bytes is larger than this threshold and also larger than `spark.sql.adaptive.skewJoin.skewedPartitionFactor` multiplying the median partition size.

Ideally, this config should be set larger than `spark.sql.adaptive.advisoryPartitionSizeInBytes`.

## spark.sql.adaptive.forceOptimizeSkewedJoin

When true, force enable OptimizeSkewedJoin, which is an adaptive rule to optimize skewed joins to avoid straggler tasks, even if it introduces extra shuffle.
