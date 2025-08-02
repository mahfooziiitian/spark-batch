package com.mahfooz.spark.join.implementation.skewjoin.ade

/**
 * AQE is a suite of runtime optimization features which is now enabled by default from Spark 3.0.
 * One of the key feature this suite packs is the capability to automatically optimize Joins for skewed Datasets.
 * AQE performs this optimization generally for ‘Sort Merge Joins’ of a skewed dataset with a non- skewed dataset.
 * AQE operates at partitioning step of a Sort Merge Join where the two input Datasets are firstly partitioned based on corresponding Join Key.
 * After the shuffle blocks are written by the MapTasks during partitioning, Spark Execution Engine gets stats on the size of each shuffled partitions.
 * With these stats available from Spark Execution Engine, AQE can determine, in tandem with certain configurable parameters, if certain partitions are skewed or not.
 * In case certain partitions are found as skewed, AQE breaks down these partitions into smaller partitions.
 * This breakdown is controller by a set of configurable parameters.
 * The smaller partitions resulting from the breakdown of a bigger skewed partition are then joined with a copy of corresponding partition of the other non skewed input dataset.
 *
 * Following are the config parameters that affect skewed join optimization feature in AQE:

"spark.sql.adaptive.skewJoin.enabled" : This boolean parameter controls whether skewed join optimization is turned on or off. Default value is true.

"spark.sql.adaptive.skewJoin.skewedPartitionFactor": This integer parameter controls the interpretation of a skewed partition. Default value is 5.

"spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": This parameter in MBs also controls the interpretation of a skewed partition. Default value is 256 MB.

 A partition is considered skewed when both (partition size > skewedPartitionFactor * median partition size) and (partition size > skewedPartitionThresholdInBytes) are true.
 AQE like ‘Broadcast Hash Join’ and ‘Salted Sort Merge Join’ cannot handle ‘Full Outer Join’. Also, it cannot handle skewedness on both the input dataset. Therefore, as in case of ‘Salted Sorted Merge Join’, AQE can handle skew only in the left dataset in the Left Joins category (Outer, Semi and Anti) and skew in the right dataset in the Right Joins category.

 */
object SkewJoinADE {

  def main(args: Array[String]): Unit = {

  }

}
