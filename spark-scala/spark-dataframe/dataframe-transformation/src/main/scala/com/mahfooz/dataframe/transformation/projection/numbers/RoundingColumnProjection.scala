package com.mahfooz.dataframe.transformation.projection.numbers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{bround, col, lit, round}

object RoundingColumnProjection {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("RoundingColumnProjection")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.select(round(col("UnitPrice"), 1)
      .alias("rounded"), col("UnitPrice"))
      .show(5)

    df.select(round(lit("2.5")), bround(lit("2.5")))
      .show(2)
  }

}
