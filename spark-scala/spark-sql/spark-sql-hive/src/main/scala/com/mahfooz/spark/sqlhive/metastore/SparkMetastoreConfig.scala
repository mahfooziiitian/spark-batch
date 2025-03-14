package com.mahfooz.spark.sqlhive.metastore

import org.apache.spark.sql.SparkSession

object SparkMetastoreConfig {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")
    System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home", ""))

    val spark = SparkSession
      .builder()
      .appName("SparkMetastoreConfig")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    //println(s"${spark.conf.get("spark.sql.hive.metastore.version")}")
    //println(s"${spark.conf.get("spark.sql.hive.metastore.jars")}")
    //println(s"${spark.conf.get("spark.sql.hive.metastore.sharedPrefixes")}")


  }

}
