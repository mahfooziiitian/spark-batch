package com.mahfooz.spark.math.statistics
import org.apache.spark.sql.SparkSession

object ApproxPercentile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ApproximatePercentile")
      .getOrCreate()

    spark
      .sql(
        " SELECT approx_percentile(col, array(0.5, 0.4, 0.1), 100) FROM VALUES (0), (1), (2), (10) AS tab(col)"
      )
      .show()
    spark
      .sql(
        "SELECT approx_percentile(col, 0.5, 100) FROM VALUES (0), (6), (7), (9), (10) AS tab(col)"
      )
      .show()

    spark.stop()
  }
}
