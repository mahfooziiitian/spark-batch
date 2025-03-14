package com.mahfooz.spark.jdbc.predicates

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Predicates {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Predicates")
      .master("local")
      .getOrCreate()

    val conf = ConfigFactory.load("db.conf")
    val url=conf.getString("db.url")
    val userName=conf.getString("db.userName")
    val password=conf.getString("db.password")
    val driver=conf.getString("db.driver")

    val tablename = "user_tables"

    val props = new java.util.Properties
    props.setProperty("user",userName)
    props.setProperty("password",password)
    props.setProperty("driver",driver)

    val predicates = Array(
      "TABLESPACE_NAME = 'USERS' AND TABLE_NAME = 'ROM_MOVE_DTL_REJECT'")
    spark.read.jdbc(url, tablename, predicates, props).show()
    spark.read.jdbc(url, tablename, predicates, props).rdd.getNumPartitions // 2
  }
}
