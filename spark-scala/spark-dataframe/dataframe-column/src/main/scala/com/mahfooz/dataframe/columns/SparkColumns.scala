package com.mahfooz.dataframe.columns

import org.apache.spark.sql.SparkSession

object SparkColumns {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkColumns")
      .master("local")
      .getOrCreate()

    val columns = spark.read
      .format("json")
      .load("D:\\data\\flight-data\\json\\2015-summary.json")
      .columns

    columns.foreach(println)
  }
}
