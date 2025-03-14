package com.mahfooz.spark.rdd.operation.action.take

import org.apache.spark.sql.SparkSession

object TakeAction {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("TakeAction")
      .master("local")
      .getOrCreate()

    val data = spark.read
      .textFile("D:\\data\\spark\\text\\spark_test.txt")
      .rdd

    val words = data.flatMap(lines => lines.split(" "))
    words.take(5).foreach(println)
    words.takeOrdered(5).foreach(println)
    words.top(5).foreach(println)
    val withReplacement = true
    val numberToTake = 6
    val randomSeed = 100L
    words.takeSample(withReplacement, numberToTake, randomSeed).foreach(println)
  }
}
