package com.mahfooz.spark.ds.multiline

import org.apache.spark.sql.SparkSession

object SparkMultilineJson {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val movies =
      spark.read
        .option("multiLine", true)
        .json(args(0))

    movies.show(10)
  }
}
