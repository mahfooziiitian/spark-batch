package com.mahfooz.spark.df.ds.text

import org.apache.spark.sql.SparkSession

object TextFileReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val textFile = spark.read.text("D:\\data\\spark\\spark-by-examples\\txt\\holmes.txt")

    textFile.printSchema

    textFile.show(5, false)

    spark.stop()

  }
}
