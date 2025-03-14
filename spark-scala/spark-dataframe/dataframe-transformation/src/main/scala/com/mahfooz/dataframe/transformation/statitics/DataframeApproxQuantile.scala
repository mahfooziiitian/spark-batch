package com.mahfooz.dataframe.transformation.statitics

import org.apache.spark.sql.SparkSession

object DataframeApproxQuantile {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("DataframeApproxQuantile")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    val colName = "UnitPrice"
    val quantileProb = Array(0.5)
    val relError = 0.05

    println(df.stat.approxQuantile(colName, quantileProb, relError))
  }

}
