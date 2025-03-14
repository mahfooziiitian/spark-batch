package com.mahfooz.dataframe.transformation.projection.strings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lower, upper}

object StringProjection {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("StringProjection")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    /*
    SELECT initcap(Description) FROM dfTable
     */
    df.select(initcap(col("Description")))
      .show(2, false)

    /*
    SELECT Description, lower(Description), Upper(lower(Description)) FROM dfTable
     */
    df.select(col("Description"),
      lower(col("Description")),
      upper(lower(col("Description")))).show(2)
  }

}
