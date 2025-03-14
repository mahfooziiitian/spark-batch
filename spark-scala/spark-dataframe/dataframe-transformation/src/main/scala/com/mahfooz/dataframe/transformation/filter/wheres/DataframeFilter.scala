package com.mahfooz.dataframe.transformation.filter.wheres

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DataframeFilter {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataframeFilter")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)

  }

}
