package com.mahfooz.sparksql.generic.aggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object AggregationFunction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(args(0))

    flightData2015.createOrReplaceTempView("flight_data_2015")

    //Query on temporary view
    spark.sql("SELECT max(count) from flight_data_2015")
      .take(1)
      .foreach(println)

    //Using max function
    flightData2015.select(max("count"))
      .take(1)
      .foreach(println)

  }
}
