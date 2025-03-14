package com.mahfooz.datafram.aggregation.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, rank}

object SparkWindow {

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkWindow")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = start()
    val statesPopulationDF =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("D:\\data\\processing\\batch\\spark\\csv\\statesPopulation.csv")

    val windowSpec = Window
      .partitionBy("State")
      .orderBy(col("Population").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    statesPopulationDF
      .select(
        col("State"),
        col("Year"),
        max("Population").over(windowSpec),
        rank().over(windowSpec)
      )
      .sort("State", "Year")
      .show(10)

  }

}
