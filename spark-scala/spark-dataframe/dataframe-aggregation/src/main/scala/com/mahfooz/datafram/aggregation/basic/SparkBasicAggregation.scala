package com.mahfooz.datafram.aggregation.basic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, countDistinct}

object SparkBasicAggregation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkBasicAggregation")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.select(count("StockCode")).show()

    df.select(countDistinct("StockCode")).show()

  }

}
