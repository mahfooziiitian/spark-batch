package com.mahfooz.spark.sqlhive.context

import org.apache.spark.sql.SparkSession

object HiveContextSql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("hive-context-sql")
      .master("local[*]")
      .getOrCreate()

  }

}
