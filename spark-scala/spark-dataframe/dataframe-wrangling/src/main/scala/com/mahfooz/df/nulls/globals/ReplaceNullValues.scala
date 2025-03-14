/*
In addition to replacing null values like we did with drop and fill, there are more flexible options that you can use with more than just null values. Probably the most common use case is to replace all values in a certain column according to their current value.

 */
package com.mahfooz.df.nulls.globals

import org.apache.spark.sql.SparkSession

object ReplaceNullValues {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ReplaceNullValues")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.na.replace("Description", Map("" -> "UNKNOWN")).show()

  }
}
