package com.mahfooz.spark.datetime.timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateTime {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateTime")
      .getOrCreate()

    import spark.implicits._

    // the last two columns don't follow the default date format
    val testDateTSDF = Seq(
      (
        1,
        "2018-01-01",
        "2019-07-01 12:01:19.000",
        "01-01-2018",
        "12-05-2017 45:50"
      )
    ).toDF("id", "date", "timestamp", "date_str", "ts_str")

    testDateTSDF.printSchema()

    //Convert these strings into date, timestamp and unix timestamp and specify a custom date and timestamp format
    val testDateResultDF = testDateTSDF
      .select(
        to_date('date).as("date1"),
        to_timestamp('timestamp).as("ts1"),
        to_date('date_str, "MM-dd-yyyy").as("date2"),
        to_timestamp('ts_str, "MM-dd-yyyy mm:ss").as("ts2"),
        unix_timestamp('timestamp).as("unix_ts")
      )

    testDateResultDF.show(false)

    // date1 and ts1 are of type date and timestamp respectively
    testDateResultDF.printSchema

    //Date & Timestamp into string
    testDateResultDF
      .select(
        date_format('date1, "dd-MM-YYYY").as("date_str"),
        date_format('ts1, "dd-MM-YYYY HH:mm:ss").as("ts_str"),
        from_unixtime('unix_ts, "dd-MM-YYYY HH:mm:ss").as("unix_ts_str")
      )
      .show

  }
}
