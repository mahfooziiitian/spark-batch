package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object ProjectColumAlias {

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

    // in Scala
    df.select(
        expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")
      )
      .show(2)
  }

}
