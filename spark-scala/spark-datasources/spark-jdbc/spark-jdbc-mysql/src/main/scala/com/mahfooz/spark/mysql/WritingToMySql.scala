package com.mahfooz.spark.mysql

import org.apache.spark.sql.{SaveMode, SparkSession}

object WritingToMySql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("WritingToMySql")
      .master("local[*]")
      .getOrCreate()

    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/sakila"
    val tableName = "film"

    val props = new java.util.Properties
    props.setProperty("driver", driver)
    props.setProperty("url", url)
    props.setProperty("user", "root")
    props.setProperty("password", "MySQL_2023")

    val predicates = Array("rating in ('PG', 'G'")

    val dbDataFrame = spark.read.jdbc(url = url,
      table = tableName,
      predicates = predicates, connectionProperties = props)

    val newTableName =  s"${tableName}_write"

    dbDataFrame.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", url)
      .option("dbtable", newTableName)
      .option("user", "root")
      .option("password", "MySQL_2023")
      .save()
  }

}
