package com.mahfooz.spark.mysql.pushdown.query

import org.apache.spark.sql.SparkSession

object PushDownQuery {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("PushDownQuery")
      .master("local[*]")
      .getOrCreate()

    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/sakila"
    val tableName = "film"

    val pushDownQuery =
      s"""SELECT DISTINCT (rental_duration) FROM ${tableName}""".stripMargin

    val dbDataFrame = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", "root")
      .option("query", pushDownQuery)
      .option("password", "MySQL_2023")
      .load()


    dbDataFrame.explain(true)

  }

}
