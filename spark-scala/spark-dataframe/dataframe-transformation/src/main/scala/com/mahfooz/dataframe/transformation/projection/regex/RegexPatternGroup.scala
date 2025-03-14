package com.mahfooz.dataframe.transformation.projection.regex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract}

object RegexPatternGroup {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RegexPatternGroup")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
    // the | signifies OR in regular expression syntax
    df.select(
        regexp_extract(col("Description"), regexString, 1).alias("color_clean"),
        col("Description")
      )
      .show(2)
    /*
      SELECT regexp_extract(Description, '(BLACK|WHITE|RED|GREEN|BLUE)', 1),
        Description
      FROM dfTable
     */
    val extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
    df.select(
        regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
        col("Description")
      )
      .show(2)
  }

}
