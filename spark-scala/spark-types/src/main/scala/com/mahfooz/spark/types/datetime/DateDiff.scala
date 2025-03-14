package com.mahfooz.spark.types.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, lit, date_sub}
import org.apache.spark.sql.functions.{datediff, months_between, to_date}

object DateDiff {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("DateDiff")
      .master("local")
      .getOrCreate()

    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())


    dateDF.withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today"))).show(1)

    dateDF.select(
      to_date(lit("2016-01-01")).alias("start"),
      to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("start"), col("end"))).show(1)
  }
}
