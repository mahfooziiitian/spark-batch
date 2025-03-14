package com.mahfooz.spark.rdd.operation.transformation.narrow.map.partition

import org.apache.spark.sql.SparkSession

object MapPartition {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MapPartition")
      .master("local")
      .getOrCreate()

    val fileRDD = spark.sparkContext.wholeTextFiles(
      getClass
        .getResource("/spark_test.txt")
        .getFile
    )

    val words = fileRDD.flatMap(line => line._2.split(" "))
    println(
      words
        .mapPartitions(part => Iterator[Int](1))
        .sum()
    )

  }
}
