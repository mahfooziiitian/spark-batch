package com.mahfooz.spark.whens

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when

object WhenFunction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WhenFunction")
      .getOrCreate()

    import spark.implicits._

    // create a DataFrame with values from 1 to 7 to represent each day of the week
    val dayOfWeekDF = spark.range(1, 8, 1)

    // convert each numerical value to a string
    dayOfWeekDF
      .select(
        'id,
        when('id === 1, "Mon")
          .when('id === 2, "Tue")
          .when('id === 3, "Wed")
          .when('id === 4, "Thu")
          .when('id === 5, "Fri")
          .when('id === 6, "Sat")
          .when('id === 7, "Sun")
          .as("dow")
      )
      .show

    dayOfWeekDF
      .select(
        'id,
        when('id === 6, "Weekend")
          .when('id === 7, "Weekend")
          .otherwise("Weekday")
          .as("day_type")
      )
      .show

  }

}
