/*

HashPartitioner is the default partitioner in Spark and works by calculating a hash value for each key of
the RDD elements.
All the elements with the same hashcode end up in the same partition.

partitionIndex = hashcode(key) % numPartitions

The default number of partitions is either from the Spark configuration parameter
spark.default.parallelism or the number of cores in the cluster.

 */
package com.mahfooz.spark.rdd.partition.partitioners.hash

object HashPartitionerEx {}
