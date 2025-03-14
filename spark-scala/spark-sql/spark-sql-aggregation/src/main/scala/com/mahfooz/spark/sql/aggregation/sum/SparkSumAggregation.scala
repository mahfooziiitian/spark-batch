package com.mahfooz.spark.sql.aggregation.sum

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object SparkSumAggregation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSumAggregation")
      .getOrCreate()

    val statesDF = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("sep", ",")
      .csv("D:\\data\\spark\\csv\\statesPopulation.csv")

    statesDF
      .groupBy("State")
      .agg(
        sum("Population")
          .alias("Total")
      )
      .show(5)
  }
}
