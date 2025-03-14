package com.mahfooz.spark.types.string

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap,lower,upper}

object SparkString {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SparkString")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.select(initcap(col("Description")))
      .show(2, false)

    df.select(col("Description"),  lower(col("Description")),
      upper(lower(col("Description")))).show(2)
  }
}
