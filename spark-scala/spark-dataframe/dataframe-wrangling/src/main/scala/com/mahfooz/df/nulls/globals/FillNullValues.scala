/*
Using the fill function, you can fill one or more columns with a set of values.
This can be done by specifying a map—that is a particular value and a set of columns.

 */
package com.mahfooz.df.nulls.globals

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FillNullValues {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("FillNullValues")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.na.fill(5, Seq("StockCode", "InvoiceNo"))
      .where(col("StockCode") === 5)
      .show()

  }

}
