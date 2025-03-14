/*
groupByKey groups the values for each key in the RDD into a single sequence.
groupByKey also allows controlling the partitioning of the resulting key-value pair RDD by passing a partitioner.
By default, a HashPartitioner is used but a custom partitioner can be given as an argument.
The ordering of elements within each group is not guaranteed, and may even differ each time the resulting RDD is evaluated
groupByKey is an expensive operation due to all the data shuffling needed.
reduceByKey or aggregateByKey provide much better performance.

groupByKey can be invoked either using a custom partitioner or just using the default HashPartitioner as shown in the following code snippet:

def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]

def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]

 */

package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.sql.SparkSession

object GroupByKeys {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("GroupByKeys")
      .master("local")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(
      List(
        "Spark",
        "is",
        "an",
        "amazing",
        "piece",
        "of",
        "technology",
        "and ",
        "Processing"
      )
    )

    val pairRDD = rdd.map(words => (words.length, words))

    val wordByLenRDD = pairRDD.groupByKey()

    wordByLenRDD.collect().foreach(println)
  }
}
