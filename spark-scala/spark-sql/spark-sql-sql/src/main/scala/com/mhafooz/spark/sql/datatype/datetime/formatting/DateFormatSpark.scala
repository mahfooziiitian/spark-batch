package com.mhafooz.spark.sql.datatype.datetime.formatting

import org.apache.spark.sql.SparkSession

object DateFormatSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("DateFormatSpark")
      .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

    import spark.implicits._

    // the last two columns don't follow the default date format
    val df = Seq(
      (
        1,
        "2018-01-01",
        "2018-01-01 15:04:58.865",
        "01-01-2018",
        "12-05-2017 45:50"
      )
    ).toDF("id", "date", "timestamp", "date_str", "ts_str")

    df.show(truncate = false)

    df.createOrReplaceTempView("date_time")

    val df_date = spark.sql(
      """
        |SELECT  to_date(date) as to_date_func,
        |        to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') as to_timestamp_func,
        |        to_date(date_str, 'MM-dd-yyyy') as to_date_format,
        |        to_timestamp(ts_str, 'MM-dd-yyyy mm:ss') as to_timestamp_format,
        |        unix_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') as unix_timestamp_func
        |FROM date_time
        |""".stripMargin
    )

    df_date.show(truncate = false)

    df_date.createOrReplaceTempView("date_time_formatted")

    val df_date_str = spark.sql(
      """
      |SELECT date_format(to_date_func, 'dd-MM-yyyy') as date_str,
      | date_format(to_timestamp_func, 'dd-MM-yyyy HH:mm:ss') as ts_str,
      | from_unixtime(unix_timestamp_func, 'dd-MM-yyyy HH:mm:ss') as unix_ts_str
      |FROM date_time_formatted""".stripMargin
    )

    df_date_str.show(truncate = false)

  }
}
