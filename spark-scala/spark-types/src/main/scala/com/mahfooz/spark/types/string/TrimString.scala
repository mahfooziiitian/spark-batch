package com.mahfooz.spark.types.string


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, lpad, ltrim, rpad, rtrim, trim}

object TrimString {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("TrimString")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.select(    ltrim(lit("    HELLO    ")).as("ltrim"),
      rtrim(lit("    HELLO    ")).as("rtrim"),
      trim(lit("    HELLO    ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lp"),
      rpad(lit("HELLO"), 10, " ").as("rp")).show(2)
  }
}
