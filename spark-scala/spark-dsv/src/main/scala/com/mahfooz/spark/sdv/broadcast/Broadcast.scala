/*

Broadcast variables let you save a large value on all the worker nodes and reuse it across many Spark actions
without re-sending it to the cluster.

 */
package com.mahfooz.spark.sdv.broadcast

import org.apache.spark.sql.SparkSession

object Broadcast {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .master("local")
      .appName("Broadcast")
      .getOrCreate()

    val supplementalData = Map("Spark" -> 1000, "Definitive" -> 200,
      "Big" -> -300, "Simple" -> 100)

    val sc=spark.sparkContext

    val suppBroadcast = sc.broadcast(supplementalData)
    println(suppBroadcast.value)

    val my_collection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")

    val words = sc.parallelize(my_collection, 2)

    words.map(word => (word, suppBroadcast.value.getOrElse(word, 0)))
      .sortBy(wordPair => wordPair._2)
      .collect()
      .foreach(println)

  }
}
