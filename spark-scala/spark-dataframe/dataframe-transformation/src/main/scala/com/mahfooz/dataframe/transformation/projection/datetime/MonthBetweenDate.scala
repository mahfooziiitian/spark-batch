/*

 */
package com.mahfooz.dataframe.transformation.projection.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, lit, months_between, to_date}

object MonthBetweenDate {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MonthBetweenDate")
      .getOrCreate()

    val dateDF = spark
      .range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    /*
    SELECT to_date('2016-01-01'), months_between('2016-01-01', '2017-01-01'),
    datediff('2016-01-01', '2017-01-01')
    FROM dateTable
     */
    dateDF
      .select(
        to_date(lit("2016-01-01")).alias("start"),
        to_date(lit("2017-05-22")).alias("end")
      )
      .select(months_between(col("start"), col("end")))
      .show(1)
  }

}
