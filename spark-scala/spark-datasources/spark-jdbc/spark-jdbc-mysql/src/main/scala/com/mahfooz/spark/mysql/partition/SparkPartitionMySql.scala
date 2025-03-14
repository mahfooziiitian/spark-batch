package com.mahfooz.spark.mysql.partition

import org.apache.spark.sql.SparkSession

object SparkPartitionMySql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MysqlSparkDatasource")
      .master("local[*]")
      .getOrCreate()

    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/sakila"
    val tableName = "film"

    val dbDataFrame = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", tableName)
      .option("driver", driver)
      .option("user", "root")
      .option("partitionColumn","rental_duration")
      .option("lowerBound","1")
      .option("upperBound","9")
      .option("numPartitions",8)
      .option("password", "MySQL_2023")
      .load()

    dbDataFrame
      .where("film_id<2000")
      .explain(true)

    dbDataFrame.show(1000)

    println(dbDataFrame.rdd.getNumPartitions)

  }
}
