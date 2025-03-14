package com.mahfooz.spark.jdbc.predicates

import com.mahfooz.spark.jdbc.config.ConfigUtil
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Predicates {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("spark-jdbc-predicates")
      .master("local[*]")
      .getOrCreate()

    val tableName = "master.sys.all_views"

    val config = ConfigFactory.load("db.conf")
    val props = ConfigUtil.getOdbcProperty(config)

    val predicates = Array("type_desc = 'VIEW'")
    val sparkJdbc = spark.read.jdbc(props.getProperty("url"),tableName, predicates, props)
    sparkJdbc.show(false)
    println(sparkJdbc.rdd.getNumPartitions) // 2
  }
}
