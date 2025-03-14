package com.mahfooz.dataframe.transformation.projection.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_add, date_sub}

object DateAddSubtract {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("DateSubtract")
      .getOrCreate()

    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    /*
    SELECT date_sub(today, 5), date_add(today, 5) FROM dateTable
     */
    dateDF.select(date_sub(col("today"), 5),
      date_add(col("today"), 5))
      .show(1)

  }

}
