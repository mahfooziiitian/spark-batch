package com.mahfooz.dataframe.transformation.projection.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  col,
  current_date,
  current_timestamp,
  date_sub,
  datediff,
  lit,
  months_between,
  to_date
}

object DateDifference {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateDifference")
      .getOrCreate()

    val dateDF = spark
      .range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    dateDF
      .withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today")))
      .show(1)


  }

}
