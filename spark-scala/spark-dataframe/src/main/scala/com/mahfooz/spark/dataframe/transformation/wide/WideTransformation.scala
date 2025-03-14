/*

A wide dependency (or wide transformation) style transformation will have input partitions contributing to many
output partitions.
You will often hear this referred to as a shuffle whereby Spark will exchange partitions across the cluster.
With narrow transformations, Spark will automatically perform an operation called pipelining, meaning that if we
specify multiple filters on DataFrames, they’ll all be performed in-memory.
The same cannot be said for shuffles.
When we perform a shuffle, Spark writes the results to disk.

 */
package com.mahfooz.spark.dataframe.transformation.wide

object WideTransformation {

}
