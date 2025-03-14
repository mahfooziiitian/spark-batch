package com.mahfooz.spark.storage.memory

import org.apache.spark.sql.SparkSession

object SparkStorage {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SaveAsTextFiles")
      .master("local[*]")
      .getOrCreate()

    val data = spark.read
      .textFile(args(0))
      .rdd

    val words=data.flatMap(line => line.split(" "));
    println(words.getStorageLevel)

  }
}
