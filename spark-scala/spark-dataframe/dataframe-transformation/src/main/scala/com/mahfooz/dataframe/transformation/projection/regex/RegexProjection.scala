package com.mahfooz.dataframe.transformation.projection.regex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace}

object RegexProjection {

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

    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("|")
    // the | signifies `OR` in regular expression syntax
    /*
    SELECT
      regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'COLOR') as
      color_clean, Description
    FROM dfTable
    */
    df.select(
      regexp_replace(col("Description"), regexString, "COLOR")
        .alias("color_clean"),
      col("Description")).show(2)
  }

}
