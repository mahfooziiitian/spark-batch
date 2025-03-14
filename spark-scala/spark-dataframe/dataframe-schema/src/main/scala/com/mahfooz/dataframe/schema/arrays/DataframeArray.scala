package com.mahfooz.dataframe.schema.arrays

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, size, split}

object DataframeArray {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataframeArray")
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME","data")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$dataHome/FileData/Csv/retail-data/all/online-retail-dataset.csv")

    /*
    SELECT split(Description, ' ') FROM dfTable
     */
    df.select(split(col("Description"), " "))
      .show(2)

    /*
    SELECT split(Description, ' ')[0] FROM dfTable
     */
    df.select(split(col("Description"), " ").alias("array_col"))
      .selectExpr("array_col[0]")
      .show(2)

    df.select(size(split(col("Description"), " "))).show(2)

    /*
    SELECT array_contains(split(Description, ' '), 'WHITE') FROM dfTable
     */
    df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

  }

}
