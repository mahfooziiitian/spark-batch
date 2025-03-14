package com.mahfooz.spark.rdd.datasource.parallelize

import org.apache.spark.sql.SparkSession

object Parallelize {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName ("Parallelize")
      .master ("local")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").getOrCreate ()
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val lines = spark.read.textFile("spark-rdd/src/main/resources/spark_test.txt").rdd
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    println(totalLength)
  }
}
