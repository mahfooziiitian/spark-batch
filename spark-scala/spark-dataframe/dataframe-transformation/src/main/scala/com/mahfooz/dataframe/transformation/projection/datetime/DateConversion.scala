package com.mahfooz.dataframe.transformation.projection.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_date}

object DateConversion {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateConversion")
      .getOrCreate()

    //string to date conversion
    spark.range(5).withColumn("date", lit("2017-01-01"))
      .select(to_date(col("date")).as("date")).show(1)

  }

}
