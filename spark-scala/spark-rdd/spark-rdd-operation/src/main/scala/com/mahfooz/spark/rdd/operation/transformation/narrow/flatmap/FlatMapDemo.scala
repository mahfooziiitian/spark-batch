package com.mahfooz.spark.rdd.operation.transformation.narrow.flatmap

import org.apache.spark.sql.SparkSession

object FlatMapDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName ("FlatMapDemo")
      .master ("local")
      .config("spark.sql.warehouse.dir","D:\\data\\processing\\spark-warehouse")
      .config("hive.metastore.warehouse.dir","D:\\data\\processing\\spark-warehouse")
      .getOrCreate ()

    val lines = spark.read.textFile("D:\\data\\processing\\batch\\spark\\text\\spark_test.txt").rdd

    lines.foreach(println)

    val flatmapFile = lines.flatMap(lines => lines.split(" "))
    flatmapFile.foreach(println)
  }
}
