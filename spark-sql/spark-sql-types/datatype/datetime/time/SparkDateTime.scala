package com.mhafooz.spark.sql.datatype.datetime.time

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDateTime {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("SparkDateTime")
      .getOrCreate()

    import spark.implicits._

    val employeeData = Seq(
      ("John", "2016-01-01", "2017-10-15"),
      ("May", "2017-02-06", "2017-12-25")
    ).toDF("name", "join_date", "leave_date")

    employeeData.show

    // perform date and month calcuations
    employeeData
      .select(
        'name,
        datediff('leave_date, 'join_date).as("days"),
        months_between('leave_date, 'join_date).as("months"),
        last_day('leave_date).as("last_day_of_mon")
      )
      .show
    // perform date addition and substration

    val oneDate = Seq(("2018-01-01")).toDF("new_year")
    oneDate
      .select(
        date_add('new_year, 14).as("mid_month"),
        date_sub('new_year, 1).as("new_year_eve"),
        next_day('new_year, "Mon").as("next_mon")
      )
      .show

    val valentimeDateDF = Seq(("2018-02-14 05:35:55")).toDF("date")

    valentimeDateDF.select(
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

  }
}
