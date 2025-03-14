/*

By default, the keys are sorted in ascending order.

This transformation is simple to understand.

It sorts the rows according the key, and there is an option to specify whether the result should be in ascending
(default) or descending order.

 */
package com.mahfooz.spark.rdd.operation.transformation.wide.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

object SortByKeys {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local")

    val sc = new SparkContext(conf)

    val candyTx = sc.parallelize(
      List(
        ("candy1", 5.2),
        ("candy2", 3.5),
        ("candy1", 2.0),
        ("candy2", 6.0),
        ("candy3", 3.0)
      )
    )

    val summaryTx = candyTx.reduceByKey((total, value) => total + value)
    summaryTx.collect().foreach(println)

    val summaryByPrice = summaryTx.map(t => (t._2, t._1)).sortByKey()
    summaryByPrice.collect.foreach(println)
  }
}
