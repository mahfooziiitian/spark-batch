package com.mahfooz.spark.jdbc.oracle

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object OracleSpark {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load("db.conf")
    val url = conf.getString("db.url")
    val userName = conf.getString("db.userName")
    val password = conf.getString("db.password")
    val driver = conf.getString("db.driver")

    val tablename = "user_tables"

    val spark = SparkSession
      .builder()
      .appName("OracleSpark")
      .master("local")
      .getOrCreate()

    val dbDataFrame = spark.read
      .format("jdbc")
      .option("url", url)
      .option("user", userName)
      .option("password", password)
      .option("dbtable", tablename)
      .option("driver", driver)
      .load()

    dbDataFrame.show(10)
  }
}
