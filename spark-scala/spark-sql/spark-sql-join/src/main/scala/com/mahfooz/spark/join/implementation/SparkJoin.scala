/*

Joining is one of the most expensive operations in Spark.
At a high level, there are two different strategies Spark uses to join two datasets.
They are shuffle hash join and broadcast join.
The main criteria for selecting a particular strategy is based on the size of the two datasets.
When the size of both datasets is large, then the shuffle hash join strategy is used.
When the size of one of the datasets is small enough to fit into the memory of the executors, then the broadcast join strategy is used.

 */
package com.mahfooz.spark.join.implementation

object SparkJoin {}
