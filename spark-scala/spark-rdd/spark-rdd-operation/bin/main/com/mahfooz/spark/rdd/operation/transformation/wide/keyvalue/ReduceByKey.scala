/*
First performs the grouping of values with the same key and then applies the specified func to return the list of
values down to a single value.

For a dataset of (K,V) pairs, the returned RDD has the type of (K, V).

If that processing is done using a binary operation that complies with the commutative and associated properties,
then it is best to use the reduceByKey transformation to speed up the processing logic.

This transformation is often used to reduce all the values of the same key to a single value.

groupByKey involves a lot of shuffling and reduceByKey tends to improve the performance by not sending all elements of the PairRDD using shuffles, rather using a local combiner to first do some basic aggregations locally and then send the resultant elements as in groupByKey. This greatly reduces the data transferred, as we don't need to send everything over. reduceBykey works by merging the values for each key using an associative and commutative reduce function. Of course, first, this will
also perform the merging locally on each mapper before sending results to a reducer.
If you are familiar with Hadoop MapReduce, this is very similar to a combiner in MapReduce programming.

def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)]

def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]

def reduceByKey(func: (V, V) => V): RDD[(K, V)]

reduceByKey works by sending all elements of the partitions to the partition based on the partitioner so that all pairs of (key - value) for the same Key are collected in the same partition. But before the shuffle, local aggregation is also done reducing the data to be shuffled. Once this is done, the aggregation operation can be done easily in the final partition.
 */
package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession

object ReduceByKey {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(ReduceByKey.getClass.getName)
      .master("local")
      .getOrCreate()

    val sc=spark.sparkContext

    val candyTx = sc.parallelize(List(("candy1", 5.2),
      ("candy2", 3.5),
      ("candy1", 2.0),
      ("candy2", 6.0),
      ("candy3", 3.0)))

    val summaryTx = candyTx.reduceByKey((total, value) => total + value)

    summaryTx.collect().foreach(println)

  }
}
