/*

First performs the grouping of values with the same key and then applies the specified func to return the
list of values down to a single value.
For a dataset of (K,V) pairs, the returned RDD has the type of (K, V).

 */
package com.mahfooz.spark.rdd.operation.transformation.wide.reducedbykey

import org.apache.spark.{SparkConf, SparkContext}

object ReducedByKeys {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val conf = new SparkConf().setAppName("spark-conf").setMaster("local")
    val sc=new SparkContext(conf)
    val candyTx = sc.parallelize(List(("candy1", 5.2), ("candy2", 3.5),
      ("candy1", 2.0),
      ("candy2", 6.0),
      ("candy3", 3.0)))
    val summaryTx = candyTx.reduceByKey((total, value) => total + value)
    summaryTx.collect().foreach(println)
  }
}
