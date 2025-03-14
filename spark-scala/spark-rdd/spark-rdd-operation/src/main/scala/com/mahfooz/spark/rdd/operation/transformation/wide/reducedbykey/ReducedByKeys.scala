/*

First performs the grouping of values with the same key and then applies the specified func to return the
list of values down to a single value.
For a dataset of (K,V) pairs, the returned RDD has the type of (K, V).

Though reduceByKey() triggers data shuffle, it does not change the partition count as RDD’s inherit the partition size from parent RDD.
You may get partition counts different based on your setup and how Spark creates partitions.

 */
package com.mahfooz.spark.rdd.operation.transformation.wide.reducedbykey

import org.apache.spark.{SparkConf, SparkContext}

object ReducedByKeys {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ReducedByKeys")
      .setMaster("local[*]")

    val sc=new SparkContext(conf)

    val candyTx = sc.parallelize(List(("candy1", 5.2), ("candy2", 3.5),
      ("candy1", 2.0),
      ("candy2", 6.0),
      ("candy3", 3.0)))

    println(s"parent RDD partition = ${candyTx.getNumPartitions}")

    val summaryTx = candyTx.reduceByKey((total, value) => total + value)
    println(s"child RDD partition = ${summaryTx.getNumPartitions}")


    summaryTx.collect().foreach(println)
  }
}
