package com.mahfooz.spark.math.modulas

import org.apache.spark.sql.SparkSession

object SparkModulas {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkModulas")
      .getOrCreate()

    spark
      .sql(
        "SELECT to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52')"
      )
      .show()

    spark.stop()
  }
}
