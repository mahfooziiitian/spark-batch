package com.mahfooz.spark.df.reader.text

import org.apache.spark.sql.SparkSession

object TextFileReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .config(
        "spark.sql.warehouse.dir",
        "D:\\data\\processing\\spark-warehouse"
      )
      .getOrCreate()

    val textFile = spark.read.text(
      "D:\\data\\processing\\batch\\spark\\spark-by-examples\\txt\\holmes.txt"
    )

    textFile.printSchema

    textFile.show(5, false)

    spark.stop()

  }
}
