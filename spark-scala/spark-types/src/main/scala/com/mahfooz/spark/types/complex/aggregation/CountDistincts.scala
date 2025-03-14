package com.mahfooz.spark.types.complex.aggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct

object CountDistincts {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("CountDistincts")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.select(countDistinct("StockCode")).show()
  }
}
