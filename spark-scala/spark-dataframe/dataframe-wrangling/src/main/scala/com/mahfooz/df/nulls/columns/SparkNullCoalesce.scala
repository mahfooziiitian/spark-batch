/*

coalesce function if you are from SQL or Data Warehouse background.
Coalesce function is one of the widely used function in SQL.
You can use the coalesce function to return non-null values.
The coalesce is a non-aggregate regular function in Spark SQL.
The coalesce gives the first non-null value among the given columns or null if all columns are null.
Coalesce requires at least one column and all columns have to be of the same or compatible types.

 */
package com.mahfooz.df.nulls.columns

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object SparkNullCoalesce {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkNullCoalesce")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    df.select(coalesce(col("Description"), col("CustomerId"))).show()
  }

}
