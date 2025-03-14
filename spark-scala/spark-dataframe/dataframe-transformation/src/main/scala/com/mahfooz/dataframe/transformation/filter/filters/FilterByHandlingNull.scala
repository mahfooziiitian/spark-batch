/*

One "gotcha" that can come up is if you’re working with null data when creating Boolean expressions.
If there is a null in your data, you’ll need to treat things a bit differently.

 */
package com.mahfooz.dataframe.transformation.filter.filters

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FilterByHandlingNull {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataframeFilter")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.where(
        col("Description")
          .eqNullSafe("hello")
      )
      .show()
  }

}
