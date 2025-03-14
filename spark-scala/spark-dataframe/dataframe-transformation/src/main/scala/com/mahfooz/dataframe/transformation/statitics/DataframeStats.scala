package com.mahfooz.dataframe.transformation.statitics

import org.apache.spark.sql.SparkSession

object DataframeStats {

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

    df.describe().show()

  }
}
