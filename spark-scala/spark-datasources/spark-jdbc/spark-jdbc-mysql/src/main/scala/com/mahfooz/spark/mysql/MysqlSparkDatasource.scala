package com.mahfooz.spark.mysql

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
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("dbtable", "actor")
      .option("user", "root")
      .option("password", "MySQL_2023")
      .load()

    userDF.printSchema

    userDF.select("actor_id", "first_name").show(5)

    spark.stop()

  }
}
