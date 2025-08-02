package com.mahfooz.spark.join.implementation.skewjoin

/**
 *Use broadcast join for smaller tables. If one of the tables is small enough to fit in memory, you can broadcast it to all nodes, reducing the need for shuffling.
    Consider using specialized join algorithms (e.g., map-side join) provided by some distributed processing frameworks.
 In ‘Broadcast Hash’ join, either the left or the right input dataset is broadcasted to the executor. ‘Broadcast Hash’ join is immune to skewed input dataset(s). This is due to the fact that partitioning, in accordance with ‘Join Keys’, is not mandatory on the left and the right dataset. Here, one of the dataset is broadcasted while the other can be appropriately partitioned in suitable manner to achieve uniform parallelism of any scale.
 Spark selects ‘Broadcast Hash Join’ based on the Join type and the size of input dataset(s). If the Join type is favorable and the size of dataset to be broadcasted remains below a configurable limit (spark.sql.autoBroadcastJoinThreshold (default 10 MB)), ‘Broadcast Hash Join’ is selected for executing Join. Therefore, if you increase the limit of ‘spark.sql.autoBroadcastJoinThreshold’ to a higher value so that ‘Broadcast Hash Join’ is selected only.
 One can also use broadcast hints in the SQL queries on either of the input datasets based on the Join type to force Spark to use ‘Broadcast Hash Join’ irrespective of ‘spark.sql.autoBroadcastJoinThreshold’ value.
 Therefore, if one could afford memory for the executors, ‘Broadcast Hash’ join should be adopted for faster execution of skewed join. However here are some salient points that needs to be considered while planning to use this fastest method:

    Not Applicable for Full Outer Join.
    For Inner Join, executor memory should accommodate at least smaller of the two input dataset.
    For Left , Left Anti and Left Semi Joins, executor memory should accommodate the right input dataset as the right one needs to be broadcasted.
    For Right , Right Anti and Right Semi Joins, executor memory should accommodate the left input dataset as the left one needs to be broadcasted.
    There is also a considerable demand of execution memory on executors based on the size of broadcasted dataset.
 */
object SkewOptimizedJoin {

}
