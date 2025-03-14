/*

When an RDD can be derived from one or more RDDs by transferring data over the wire or exchanging data to repartition
or redistribute the data using functions, such as aggregateByKey, reduceByKey and so on, then the child RDD is said to
depend on the parent RDDs participating in a shuffle operation.
This dependency is known as a Wide dependency as the data cannot be transformed on the same node as the one containing
the original RDD/parent RDD partition thus requiring data transfer over the wire between other executors.

 */
package com.mahfooz.spark.rdd.operation.transformation.wide

class WideTransformation {}
