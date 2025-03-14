package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ProjectColumnUsingExpr {

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("ProjectColumnUsingExpr")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = start()
    val df=spark.read.json("D:\\data\\flight-data\\json\\2015-summary.json")

    import spark.implicits._

    df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      'DEST_COUNTRY_NAME,
      $"DEST_COUNTRY_NAME",
      expr("DEST_COUNTRY_NAME"))
      .show(2)

    df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)


  }
}
