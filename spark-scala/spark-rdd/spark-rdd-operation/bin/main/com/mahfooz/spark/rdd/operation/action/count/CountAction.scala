package com.mahfooz.spark.rdd.operation.action.count

import org.apache.spark.sql.SparkSession

object CountAction {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CountAction")
      .master("local")
      .getOrCreate()

    val data = spark.read
      .textFile(
        getClass
          .getResource("/spark_test.txt")
          .getFile
      )
      .rdd

    val words = data.flatMap(lines => lines.split(" "))
    println(words.count)
  }

}
