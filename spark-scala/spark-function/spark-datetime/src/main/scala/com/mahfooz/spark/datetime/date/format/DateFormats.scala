package com.mahfooz.spark.datetime.date.format

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateFormats {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(DateFormats.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val testDateTSDF = Seq(
      (
        1,
        "2018-01-01",
        "2018-01-01 15:04:58:865",
        "01-01-2018",
        "12-05-2017 45:50"
      )
    ).toDF("id", "date", "timestamp", "date_str", "ts_str")

    val testDateResultDF = testDateTSDF.select(
      to_date('date).as("date1"),
      to_timestamp('timestamp).as("ts1"),
      to_date('date_str, "MM-dd-yyyy").as("date2"),
      to_timestamp('ts_str, "MM-dd-yyyy mm:ss").as("ts2"),
      unix_timestamp('timestamp).as("unix_ts")
    )

    testDateResultDF
      .select(
        date_format('date1, "dd-MM-YYYY").as("date_str"),
        date_format('ts1, "dd-MM-YYYY HH:mm:ss").as("ts_str"),
        from_unixtime('unix_ts, "dd-MM-YYYY HH:mm:ss").as("unix_ts_str")
      )
      .show
  }

}
