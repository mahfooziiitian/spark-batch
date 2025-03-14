package com.mahfooz.spark.types.numbers.std

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.corr

object Correlation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Correlation")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr("Quantity", "UnitPrice")).show()

  }
}
