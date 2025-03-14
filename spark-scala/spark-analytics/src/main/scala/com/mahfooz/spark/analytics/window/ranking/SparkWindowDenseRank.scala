package com.mahfooz.spark.analytics.window.ranking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkWindowDenseRank {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkWindowDenseRank")
      .getOrCreate()

    import spark.implicits._

    val dataHome = sys.env.getOrElse("DATA_HOME","flights")

    val df=spark.read
      .option("inferSchema",true)
      .option("header",true)
      .csv(s"${dataHome}/Processing/Spark/DataSources/Csv/OnlineRetail.csv")

    val dfWithDate=df.withColumn("date",to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))

    // define window specification to partition by name and order by amount in descending amount
    val forRankingWindow = Window
      .partitionBy("CustomerId","date")
      .orderBy(desc("Quantity"))

    // add a new column to contain the rank of each row, apply the rank function to rank each row
    val txDataWithRankDF =
      dfWithDate.withColumn("rank", dense_rank().over(forRankingWindow))

    // filter the rows down based on the rank to find the top 2 and display the result
    txDataWithRankDF.where('rank < 3).show(10)

  }
}
