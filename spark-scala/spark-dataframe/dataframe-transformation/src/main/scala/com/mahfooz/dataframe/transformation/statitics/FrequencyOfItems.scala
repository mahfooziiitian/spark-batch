package com.mahfooz.dataframe.transformation.statitics

import org.apache.spark.sql.SparkSession

object FrequencyOfItems {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("FrequencyOfItems")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.stat.freqItems(Seq("StockCode", "Quantity")).show(truncate = false)

  }

}
