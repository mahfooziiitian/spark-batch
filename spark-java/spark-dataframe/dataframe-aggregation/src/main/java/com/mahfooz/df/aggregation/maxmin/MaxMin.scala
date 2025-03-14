/*

Max
The maximum of the column value of one of the columns in the DataFrame. An example is if you want to find the maximum temperature of a city.

The max API has several implementations, as follows. The exact API used depends on the specific use case.

def max(columnName: String): Column
Aggregate function: returns the maximum value of the column in a group.

def max(e: Column): Column
Aggregate function: returns the maximum value of the expression in a group.

 */
package com.mahfooz.df.aggregation.maxmin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min}

object MaxMin {

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("MaxMin")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = start()
    val statesPopulationDF =
      spark.read
        .option("header", "true")
        .option("inferschema", "true")
        .csv("D:\\data\\spark\\csv\\statesPopulation.csv")

    statesPopulationDF
      .select(max("population"),
        min(col("population")),
        avg("Population"))
      .show
  }
}
