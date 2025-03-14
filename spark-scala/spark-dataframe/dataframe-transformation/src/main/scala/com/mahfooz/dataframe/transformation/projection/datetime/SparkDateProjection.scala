package com.mahfooz.dataframe.transformation.projection.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, current_timestamp}

object SparkDateProjection {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkDateProjection")
      .getOrCreate()

    val dateDF = spark.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    dateDF.printSchema()

  }

}
