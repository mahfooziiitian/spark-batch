package com.mahfooz.spark.types.re

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_replace,col}

object RE {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("RE")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0))

    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val regexString = simpleColors.map(_.toUpperCase).mkString("|")

    // the | signifies `OR` in regular expression syntax
    df.select(  regexp_replace(col("Description"), regexString,
      "COLOR").alias("color_clean"),
      col("Description")).show(2)
  }
}
