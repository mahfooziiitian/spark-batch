package com.mahfooz.spark.jdbc.parallel

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object ParallelRead {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ParallelRead")
      .getOrCreate()

    val conf = ConfigFactory
      .load("db.conf")

    val url = conf.getString("db.url")
    val userName = conf.getString("db.userName")
    val password = conf.getString("db.password")
    val driver = conf.getString("db.driver")

    val tablename = "user_tables"

    val dbDataFrame = spark.read
      .format("jdbc")
      .option("url", url)
      .option("user", userName)
      .option("password", password)
      .option("dbtable", tablename)
      .option("driver", driver)
      .option("numPartitions", 10)
      .load()

    dbDataFrame.show()

  }
}
