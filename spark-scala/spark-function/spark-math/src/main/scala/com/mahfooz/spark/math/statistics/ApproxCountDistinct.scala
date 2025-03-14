package com.mahfooz.spark.math.statistics

import org.apache.spark.sql.SparkSession

object ApproxCountDistinct {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ApproxCountDistinct")
      .getOrCreate()

    spark
      .sql(
        " SELECT approx_count_distinct(col1) FROM VALUES (1), (1), (2), (2), (3) tab(col1)"
      )
      .show()

    spark.stop()
  }
}
