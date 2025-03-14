package com.mahfooz.dataframe.transformation.projection.strings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, translate}

object StringTranslateColumn {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RegexProjection")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.select(translate(col("Description"), "LEET", "1337"),
      col("Description"))
      .show(2)
  }

}
