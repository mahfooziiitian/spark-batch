/*

When you define a table from files on disk, you are defining an unmanaged table.

 */
package com.mahfooz.spark.catalog.unmanaged

import org.apache.spark.sql.SparkSession

object UnmanagedTable {

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
      .appName("UnmanagedTable")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}" +
        "/FileData"

    spark.sql(
      s"""
        |CREATE EXTERNAL TABLE hive_flights (
        |DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |LOCATION '${dataPath}/Csv/FlightSummary/'
        |""".stripMargin)

    spark.sql(
      s"""
        |CREATE EXTERNAL TABLE hive_flights_2
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |LOCATION '${dataPath}/Csv/FlightSummary_ext/'
        |AS SELECT * FROM flights
        |""".stripMargin)

    spark.sql("select * from hive_flights").show(false)
    spark.sql("select * from hive_flights_2").show(false)
  }
}
