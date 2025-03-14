package com.mahfooz.spark.sqlhive.metastore

import org.apache.spark.sql.SparkSession

object SparkMetastore {

  def main(args: Array[String]): Unit = {
    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

    println(sparkWarehouse)

    System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home",""))

    val spark = SparkSession
      .builder()
      .appName("ShowAllPartitions")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    println(spark.version)
    println(s"spark.sql.catalogImplementation = ${spark.conf.get("spark.sql.catalogImplementation")}")
    println(s"metastore.catalog.default = ${spark.conf.get("metastore.catalog.default")}")
  }

}
