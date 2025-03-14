package com.mahfooz.spark.catalog.database

import org.apache.spark.sql.SparkSession

object SparkHiveDatabase {

  def main(args: Array[String]): Unit = {
    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")
    System.setProperty(
      "derby.system.home",
      sys.env.getOrElse("derby.system.home", "").replace("\\", "/")
    )

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkHiveDatabase")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases").show(false)

    spark.sql("CREATE DATABASE IF NOT EXISTS some_db")

    spark.sql("show databases").show(false)

    spark.sql("USE some_db")

    spark.sql("SHOW tables").show(false)

    spark.sql("SELECT current_database()").show(false)
  }

}
