package com.mahfooz.spark.rdd.operation.action.aggregation

import org.apache.spark.sql.SparkSession

object MinMax {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MinMax")
      .master("local")
      .getOrCreate()

    println("Max : " + spark.sparkContext.parallelize(1 to 20).max())
    println("Min : " + spark.sparkContext.parallelize(1 to 20).min())
  }
}
