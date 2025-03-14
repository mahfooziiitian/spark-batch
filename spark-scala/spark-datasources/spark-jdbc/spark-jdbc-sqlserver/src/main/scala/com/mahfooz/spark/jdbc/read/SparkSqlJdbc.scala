package com.mahfooz.spark.jdbc.read

import com.mahfooz.spark.jdbc.config.ConfigUtil
import org.apache.spark.sql.SparkSession

object SparkSqlJdbc {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val tableName = "master.sys.all_views"

    val jdbcProperties = ConfigUtil.getOdbcProperty("db.conf")
    val jdbcDF = spark.read
      .format("jdbc")
      .option(
        "url",
        jdbcProperties.getProperty("url")
      )
      .option("dbtable", tableName)
      .option("user", jdbcProperties.getProperty("user"))
      .option("password", jdbcProperties.getProperty("password"))
      .load()

    jdbcDF.show(false)
  }
}
