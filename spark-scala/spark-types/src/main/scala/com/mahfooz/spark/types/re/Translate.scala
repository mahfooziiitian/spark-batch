package com.mahfooz.spark.types.re

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, translate}

object Translate {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Translate")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.select(translate(col("Description"),
      "LEET", "1337"), col("Description"))
      .show(2)
  }
}
