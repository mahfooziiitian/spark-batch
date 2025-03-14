package com.mahfooz.spark.string

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkStringDs {

  def main(args: Array[String]): Unit = {

    val csvLine = "0,Warsaw,Poland"
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    val cities: Dataset[String] = Seq(csvLine).toDS
    println(cities)
  }
}
