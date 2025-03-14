package com.mahfooz.spark.types.complex.maps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, map}

object SparkMaps {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkMaps")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)


    df.select(map(col("Description"),
      col("InvoiceNo")).alias("complex_map")).show(2)
  }
}
