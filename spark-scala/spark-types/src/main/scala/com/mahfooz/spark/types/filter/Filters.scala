package com.mahfooz.spark.types.filter


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Filters {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Filters")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/data/2010-12-01.csv").getFile)

    var priceFilter = col("UnitPrice") > 600
    var descripFilter = col("Description").contains("POSTAGE")

    df.where(col("StockCode").isin("DOT"))
      .where(priceFilter.or(descripFilter))
      .show()


    val DOTCodeFilter = col("StockCode") === "DOT"
    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)

  }
}
