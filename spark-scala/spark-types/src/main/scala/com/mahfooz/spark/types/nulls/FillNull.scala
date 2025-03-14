package com.mahfooz.spark.types.nulls

import org.apache.spark.sql.SparkSession

object FillNull {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("FillNull")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.na.fill(5, Seq("StockCode", "InvoiceNo"))
    df.show()

    val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
    df.na.fill(fillColValues)
    df.show()
  }
}
