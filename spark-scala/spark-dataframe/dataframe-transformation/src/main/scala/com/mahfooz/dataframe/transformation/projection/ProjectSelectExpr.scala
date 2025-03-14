package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession

object ProjectSelectExpr {

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("ProjectColumAlias")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = start()
    val df = spark.read.json("D:\\data\\flight-data\\json\\2015-summary.json")
    df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME")
      .show(2)
  }
}
