/*

To create an RDD from a collection, you will need to use the parallelize method on a SparkContext (within a
SparkSession).
This turns a single node collection into a parallel collection.
When creating this parallel collection, you can also explicitly state the number of partitions into which
you would like to distribute this array.

 */
package com.mahfooz.rdd.collection.from

import org.apache.spark.sql.SparkSession

object RddFromCollection {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("RddFromCollection")
      .getOrCreate()

    // in Scala
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")
    val words = spark.sparkContext.parallelize(myCollection)

    // in Scala
    words.setName("myWords")
    println(words.name)

  }
}
