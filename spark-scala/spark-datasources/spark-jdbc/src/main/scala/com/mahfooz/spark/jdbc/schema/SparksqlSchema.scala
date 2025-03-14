package com.mahfooz.spark.jdbc.schema

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparksqlSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparksqlSchema")
      .getOrCreate()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "move")
    connectionProperties.put("password", "access123")
    //connectionProperties.put("customSchema", "ID DECIMAL(38, 0), COUNTRY STRING")

    val jdbcDF = spark.read
      .jdbc(
        "jdbc:oracle:thin:@mttdevdbcl03a.intl.vsnl.co.in:1521/comtst",
        "COUNTRIES",
        connectionProperties
      )

    jdbcDF.printSchema()

    jdbcDF.write
      .option(
        "createTableColumnTypes",
        "COUNTRY_ID DECIMAL(38,0), COUNTRY VARCHAR(1024)"
      )
      .jdbc(
        "jdbc:oracle:thin:@mttdevdbcl03a.intl.vsnl.co.in:1521/comtst",
        "COUNTRIES_2",
        connectionProperties
      )
  }
}
