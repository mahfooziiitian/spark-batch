package com.mahfooz.dataframe.transformation.filter.wheres

import org.apache.spark.sql.SparkSession

object DataframeFilterPredicateExpr {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataframeFilterPredicateExpr")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.where("InvoiceNo = 536365")
      .show(5, false)

    df.where("InvoiceNo <> 536365")
      .show(5, false)
  }

}
