package com.mahfooz.dataframe.expression.select

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, lit}

object SelectExprSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(SelectExprSpark.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("json")
      .load("D:\\data\\flight-data\\json\\2015-summary.json")

    // include all original columns
    df.selectExpr(
      "*",
      "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry"
    )
      .show(2)

    df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))")
      .show(2)

    df.select(expr("*"), lit(1).as("One"))
      .show(2)
  }
}
