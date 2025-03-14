package com.mahfooz.dataframe.transformation.filter.wheres

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FilterUsingColumn {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("FilterUsingColumn")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\data\\retail-data\\all\\online-retail-dataset.csv")

    val priceFilter = col("UnitPrice") > 600
    val descriptionFilter = col("Description").contains("POSTAGE")

    /*
    SELECT * FROM dfTable WHERE StockCode in ("DOT") AND(UnitPrice > 600 OR
    instr(Description, "POSTAGE") >= 1)
     */
    df.where(col("StockCode").isin("DOT"))
      .where(priceFilter.or(descriptionFilter))
      .show()

    /*
    SELECT UnitPrice, (StockCode = 'DOT' AND
    (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
    FROM dfTable
    WHERE (StockCode = 'DOT' AND
    (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))
     */

    val DOTCodeFilter = col("StockCode") === "DOT"
    df.withColumn(
        "isExpensive",
        DOTCodeFilter.and(priceFilter.or(descriptionFilter))
      )
      .where("isExpensive")
      .select("unitPrice", "isExpensive")
      .show(5)
  }

}
