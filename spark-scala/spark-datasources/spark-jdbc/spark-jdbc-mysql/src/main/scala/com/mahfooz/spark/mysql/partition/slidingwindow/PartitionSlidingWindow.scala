package com.mahfooz.spark.mysql.partition.slidingwindow

import org.apache.spark.sql.SparkSession

object PartitionSlidingWindow {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("PartitionSlidingWindow")
      .master("local[*]")
      .getOrCreate()

    val colName = "rental_duration"
    val lowerBound = 0L
    val upperBound = 348113L // this is the max count in our database
    val numPartitions = 10

    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/sakila"
    val tableName = "film"

    val props = new java.util.Properties
    props.setProperty("driver", driver)
    props.setProperty("url", url)
    props.setProperty("user", "root")
    props.setProperty("password", "MySQL_2023")

    val dataframe = spark.read.jdbc(url,
      tableName,
      colName,
      lowerBound,
      upperBound,
      numPartitions,
      props)

    dataframe.explain(true)

    println(dataframe.count())

  }

}
