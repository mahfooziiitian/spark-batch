/*

def jdbc(url: String, table: String, predicates: Array[String], connectionProperties: Properties): DataFrame
  Construct a DataFrame representing the database table accessible via JDBC URL url named table using connection properties.

def jdbc(url: String, table: String, columnName: String, lowerBound: Long, upperBound: Long, numPartitions: Int,
        connectionProperties: Properties): DataFrame
  Construct a DataFrame representing the database table accessible via JDBC URL url named table.

def jdbc(url: String, table: String, properties: Properties): DataFrame
  Construct a DataFrame representing the database table accessible via JDBC URL url named table and connection properties.

options
  url
  user
  password
  dbtable
  driver

 */
package com.mahfooz.spark.jdbc.config

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkJdbcProp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val props = new Properties()
    props.load(ClassLoader.getSystemResourceAsStream("jdbc.properties"))

    val jdbcDF2 = spark.read
      .jdbc(props.getProperty("jdbcUrl"), "COUNTRIES", props)

    //Writing to db
    jdbcDF2.write
      .jdbc(props.getProperty("jdbcUrl"), "COUNTRIES_BKP", props)

    jdbcDF2.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc(props.getProperty("jdbcUrl"), "schema.tablename", props)

  }
}
