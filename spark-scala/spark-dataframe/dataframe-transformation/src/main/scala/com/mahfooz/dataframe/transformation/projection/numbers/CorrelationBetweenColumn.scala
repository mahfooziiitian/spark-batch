package com.mahfooz.dataframe.transformation.projection.numbers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.corr

object CorrelationBetweenColumn {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("CorrelationBetweenColumn")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    /*
    SELECT corr(Quantity, UnitPrice) FROM dfTable
     */
    println(df.stat.corr("Quantity", "UnitPrice"))
    df.select(corr("Quantity", "UnitPrice")).show()
  }

}
