package com.mahfooz.spark.analytics.window.frame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, max, to_date}

object UnboundedPrecedingCurrentRow {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkWindowDenseRank")
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME", "flights")

    val df = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv(s"${dataHome}/Processing/Spark/DataSources/Csv/OnlineRetail.csv")

    val dfWithDate = df.withColumn("date",
      to_date(col("InvoiceDate"),
        "MM/d/yyyy H:mm"))

    // define window specification to partition by name and order by amount in descending amount
    val unboundedWindow = Window
      .partitionBy("CustomerId", "date")
      .orderBy(desc("Quantity"))
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    val maxPurchaseQuantity = max(col("Quantity")).over(unboundedWindow)

    dfWithDate.where("CustomerId is not NULL")
      .orderBy("CustomerId")
      .select(
      col("CustomerId"),
      col("date"),
      col("Quantity"),
      maxPurchaseQuantity.alias("maxPurchaseQuantity")
    ).show()

  }

}
