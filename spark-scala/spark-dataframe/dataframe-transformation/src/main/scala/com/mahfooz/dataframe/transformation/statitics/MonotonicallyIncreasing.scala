/*

As a last note, we can also add a unique ID to each row by using the function monotonically_increasing_id.
This function generates a unique value for each row, starting with 0.

 */
package com.mahfooz.dataframe.transformation.statitics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object MonotonicallyIncreasing {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("MonotonicallyIncreasing")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.select(monotonically_increasing_id()).show(2)
  }

}
