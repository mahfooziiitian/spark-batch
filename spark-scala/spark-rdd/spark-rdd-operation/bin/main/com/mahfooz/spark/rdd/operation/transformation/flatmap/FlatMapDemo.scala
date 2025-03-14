package com.mahfooz.spark.rdd.operation.transformation.flatmap

import org.apache.spark.sql.SparkSession

object FlatMapDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName ("FlatMapDemo")
      .master ("local")
      .getOrCreate ()

    val lines = spark.read.textFile(getClass
      .getResource("/spark_test.txt").getFile).rdd

    lines.foreach(println)

    val flatmapFile = lines.flatMap(lines => lines.split(" "))
    flatmapFile.foreach(println)
  }
}
