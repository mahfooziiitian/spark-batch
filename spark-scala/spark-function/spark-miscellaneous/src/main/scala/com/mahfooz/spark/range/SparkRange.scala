package com.mahfooz.spark.range

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkRange {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkRange")
      .getOrCreate()

    import spark.implicits._

    val numDF = spark.range(1, 11, 1, 5)

    // verify that there are 5 partitions
    println(numDF.rdd.getNumPartitions)

  }
}
