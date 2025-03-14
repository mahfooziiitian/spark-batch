package com.mahfooz.spark.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateTimeCalculate {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateTimeCalculate")
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

  }

}
