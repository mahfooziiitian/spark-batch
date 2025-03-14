package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession

object ProjectExtraColumn {

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("ProjectExtraColumn")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = start()
    val df = spark.read.json("D:\\data\\flight-data\\json\\2015-summary.json")
    // in Scala
    df.selectExpr(
        "*", // include all original columns
        "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry"
      )
      .show(2)

    df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
  }

}
