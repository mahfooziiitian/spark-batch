package com.mahfooz.spark.types.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ToDate {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ToDate")
      .master("local")
      .getOrCreate()

    spark
      .range(5)
      .withColumn("date", lit("2017-01-01"))
      .select(to_date(col("date")))
      .show(2)
  }
}
