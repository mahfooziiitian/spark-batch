package com.mahfooz.spark.analytics.window.frame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SparkWindowFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkWindowFrame")
      .getOrCreate()

    import spark.implicits._

    // small shopping transaction data set for two users
    val txDataDF = Seq(
      ("John", "2017-07-02", 13.35),
      ("John", "2017-07-06", 27.33),
      ("John", "2017-07-04", 21.72),
      ("Mary", "2017-07-07", 69.74),
      ("Mary", "2017-07-01", 59.44),
      ("Mary", "2017-07-05", 80.14)
    ).toDF("name", "tx_date", "amount")

    // define window specification to partition by name and order by amount in descending amount
    val forRankingWindow = Window
      .partitionBy("name")
      .orderBy(desc("amount"))

    // add a new column to contain the rank of each row, apply the rank function to rank each row
    val txDataWithRankDF =
      txDataDF.withColumn("rank", rank().over(forRankingWindow))

    // filter the rows down based on the rank to find the top 2 and display the result
    txDataWithRankDF.where('rank < 3).show(10)

  }
}
