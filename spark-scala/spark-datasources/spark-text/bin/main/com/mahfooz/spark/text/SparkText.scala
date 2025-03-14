package com.mahfooz.spark.text

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkText {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val lines: Dataset[String] = spark.read
      .textFile(args(0))

    lines.show()

    spark.stop()
  }
}
