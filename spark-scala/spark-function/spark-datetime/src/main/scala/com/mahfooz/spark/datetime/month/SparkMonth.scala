package com.mahfooz.spark.datetime.month

import org.apache.spark.sql.SparkSession

object SparkMonth {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkMonth")
      .getOrCreate

    spark.sql("SELECT add_months('2016-08-31', 1)").show()
    spark.stop
  }
}
