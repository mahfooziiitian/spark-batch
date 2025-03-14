package com.mahfooz.spark.rdd.datasource.jdbc

import org.apache.spark.sql.SparkSession

class SqlContextDataSources {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SqlContextDataSources")
      .getOrCreate()

    val jdbcDF = spark.sqlContext.read
      .format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:postgresql:dbserver",
          "dbtable" -> "schema.tablename",
          "partitionColumn"->"id"
        )
      )
      .load()

  }

}
