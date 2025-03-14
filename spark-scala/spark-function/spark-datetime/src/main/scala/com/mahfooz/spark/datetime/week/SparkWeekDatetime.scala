package com.mahfooz.spark.datetime.week

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkWeekDatetime {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(SparkWeekDatetime.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._
    val valentineDateDF = Seq(("2018-02-14 05:35:55")).toDF("date")

    valentineDateDF
      .select(
        year('date).as("year"),
        quarter('date).as("quarter"),
        month('date).as("month"),
        weekofyear('date).as("woy"),
        dayofmonth('date).as("dom"),
        dayofyear('date).as("doy"),
        hour('date).as("hour"),
        minute('date).as("minute"),
        second('date).as("second")
      )
      .show
  }

}
