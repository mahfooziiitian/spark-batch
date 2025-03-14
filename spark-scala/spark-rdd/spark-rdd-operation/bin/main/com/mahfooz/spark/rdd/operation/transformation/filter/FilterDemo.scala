package com.mahfooz.spark.rdd.operation.transformation.filter

import org.apache.spark.sql.SparkSession

object FilterDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("FilterDemo")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val data =
      sc.textFile("D:/data/processing/spark/spark-by-examples/txt/holmes.txt")

    val mapFile = data
      .flatMap(lines => lines.split(" "))
      .filter(word => word == "test")

    println(mapFile.count())
  }
}
