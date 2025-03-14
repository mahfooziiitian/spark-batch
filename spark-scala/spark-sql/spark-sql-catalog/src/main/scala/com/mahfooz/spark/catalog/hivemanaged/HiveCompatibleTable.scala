package com.mahfooz.spark.catalog.hivemanaged

import org.apache.spark.sql.SparkSession

object HiveCompatibleTable {

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
      .appName("HiveCompatibleTable")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS flights_hive_compatible
        |AS SELECT * FROM flights
        |""".stripMargin)

    spark.sql("select * from flights_hive_compatible").show(false)

  }
}
