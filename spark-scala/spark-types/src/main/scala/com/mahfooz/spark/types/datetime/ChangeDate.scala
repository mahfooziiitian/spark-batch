package com.mahfooz.spark.types.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  current_date,
  current_timestamp,
  date_sub,
  date_add,
  col
}

object ChangeDate {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ChangeDate")
      .master("local")
      .getOrCreate()

    val dateDF = spark
      .range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    dateDF
      .select(date_sub(col("today"), 5), date_add(col("today"), 5))
      .show(1)
  }
}
