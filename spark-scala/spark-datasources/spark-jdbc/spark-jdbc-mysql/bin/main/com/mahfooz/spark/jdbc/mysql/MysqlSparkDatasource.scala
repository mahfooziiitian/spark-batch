package com.mahfooz.spark.jdbc.mysql

import org.apache.spark.sql.SparkSession

object MysqlSparkDatasource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MysqlSparkDatasource")
      .master("local")
      .getOrCreate()

    val userDF = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://mtmdevhdoped01:3306/move")
      .option("dbtable", "users")
      .option("user", "root")
      .option("password", "root")
      .load()

    userDF.printSchema

    userDF.select("id", "name").show(5)

    spark.stop()

  }
}
