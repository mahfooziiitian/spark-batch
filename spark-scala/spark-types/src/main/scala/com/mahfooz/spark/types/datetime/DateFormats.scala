package com.mahfooz.spark.types.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_date,to_timestamp}

object DateFormats {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("DateFormats")
      .master("local")
      .getOrCreate()

    val dateFormat = "yyyy-dd-MM"
    val cleanDateDF = spark.range(1).select(
      to_date(lit("2017-12-11"), dateFormat).alias("date"),
      to_date(lit("2017-20-12"), dateFormat).alias("date2"))

    cleanDateDF.createOrReplaceTempView("dateTable2")

    cleanDateDF.show()

    cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

  }
}
