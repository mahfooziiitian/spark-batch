package com.mahfooz.spark.join.implementation.skewjoin

/**
 * ‘Broadcast MapPartitions Join’ is the only mechanism to fasten a skewed ‘Full Outer Join’ between a large skewed dataset and a smaller non-skewed dataset. In this approach, the smaller of the two input dataset is broadcasted to executors while the Join logic is manually provisioned in the ‘MapPartitions’ transformation which is invoked on the larger non-broadcasted dataset.

    Although ‘Broadcast MapPartitions Join’ supports all type of Joins and can handle skew in either or both of the dataset, the only limitation is that it requires considerable memory on executors. The larger executor memory is required to broadcast one of the smaller input dataset, and to support intermediate in-memory collection for manual Join provision.

I hope the above blog has given you a good perspective of handling skewed Joins in your Spark applications. With this background, I would encourage you all to explore one of these options whenever you encounter stragglers or memory overruns in Join stages of the your Spark applications.
 */
object BroadcastMapPartitionsJoin {

}
