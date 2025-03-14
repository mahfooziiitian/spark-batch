package com.mahfooz.dataframe.schema.row

import org.apache.spark.sql.SparkSession

object DataFrameRow {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("DataFrameRow")
      .getOrCreate()


    val dataframe=spark.read
      .option("inferSchema","true")
      .option("header","true")
      .csv("D:\\data\\flight-data\\csv\\2015-summary.csv")
      .limit(1)

    dataframe.foreach(row=> println(row.schema))

  }

}
