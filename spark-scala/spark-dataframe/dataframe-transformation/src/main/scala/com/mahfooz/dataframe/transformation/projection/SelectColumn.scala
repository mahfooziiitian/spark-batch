package com.mahfooz.dataframe.transformation.projection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object SelectColumn {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SelectColumn")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    val simpleColors = Seq("black", "white", "red", "green", "blue")
    val selectedColumns = simpleColors.map(color => {
      col("Description").contains(color.toUpperCase).alias(s"is_$color")
    }) :+ expr("*") // could also append this value

    df.select(selectedColumns: _*)
      .where(col("is_white").or(col("is_red")))
      .select("Description", "is_red", "is_white")
      .show(3, false)

  }

}
