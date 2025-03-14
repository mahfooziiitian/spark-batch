package com.mhafooz.spark.sql.datatype.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{to_date, to_timestamp, unix_timestamp}

object DataTimeSparks {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkPivoting")
      .getOrCreate()

    import spark.implicits._
    // the last two columns don't follow the default date format
    val testDateTSDF = Seq(
      (
        1,
        "2018-01-01",
        "2018-01-01 15:04:58:865",
        "01-01-2018",
        "12-05-2017 45:50"
      )
    ).toDF("id", "date", "timestamp", "date_str", "ts_str")

    // convert these strings into date, timestamp and unix timestamp
    // and specify a custom date and timestamp format
    val testDateResultDF = testDateTSDF
      .select(
        to_date('date).as("date1"),
        to_timestamp('timestamp).as("ts1"),
        to_date('date_str, "MM-dd-yyyy").as("date2"),
        to_timestamp('ts_str, "MM-dd-yyyy mm:ss").as("ts2"),
        unix_timestamp('timestamp).as("unix_ts")
      )
      .show(false)
  }
}
