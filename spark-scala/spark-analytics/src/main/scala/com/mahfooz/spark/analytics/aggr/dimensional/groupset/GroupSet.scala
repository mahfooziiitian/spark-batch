package com.mahfooz.spark.analytics.aggr.dimensional.groupset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object GroupSet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("GroupSet")
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME", "flights")

    val df = spark.read
      .option("inferSchema", value = true)
      .option("header", value = true)
      .csv(s"${dataHome}/FileData/Csv/OnlineRetail.csv")

    val dfWithDate =
      df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    val dfNotNull = dfWithDate.drop()
    dfNotNull.createOrReplaceTempView("dfNotNull")

    spark
      .sql(
        "select CustomerId, stockCode, sum(Quantity) from dfNotNull " +
          " group by CustomerId, stockCode " +
          "Grouping sets((CustomerId,stockCode),())" +
          " Order by CustomerId desc, stockCode desc"
      )
      .show()
  }

}
