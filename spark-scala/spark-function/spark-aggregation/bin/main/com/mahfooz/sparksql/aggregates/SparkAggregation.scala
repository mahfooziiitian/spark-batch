package com.mahfooz.sparksql.aggregates

import org.apache.spark.sql.SparkSession

object SparkAggregation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkAggregation")
      .getOrCreate()
    spark
      .sql(
        "SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x,acc->acc*10) as aggregate"
      )
      .show()
    spark.stop()
  }
}
