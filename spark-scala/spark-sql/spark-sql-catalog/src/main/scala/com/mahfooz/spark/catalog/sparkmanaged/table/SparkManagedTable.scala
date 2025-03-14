package com.mahfooz.spark.catalog.sparkmanaged.table

import org.apache.spark.sql.SparkSession

object SparkManagedTable {

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
      .appName("SparkManagedTable")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}" +
        "/FileData"

    val query =
      s"""
         |CREATE TABLE  if not exists flights(
         |DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
         |USING JSON OPTIONS (path '${dataPath}/Json/Flight/2015-summary.json')
         |""".stripMargin
    println(query)
    spark.sql(query)

    spark.sql("select * from flights").show(false)

    spark.sql(
      s"""
        |CREATE TABLE if not exists flights_csv (
        |DEST_COUNTRY_NAME STRING,
        |ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
        |count LONG)
        |USING csv OPTIONS (header true, path '${dataPath}/Csv/Flight/2015-summary.csv')
        |""".stripMargin)

    spark.sql("select * from flights_csv").show(false)

    spark.sql(
      """CREATE TABLE if not exists flights_from_select
        |USING parquet AS SELECT * FROM flights""".stripMargin)

    spark.sql("select * from flights_from_select").show(false)

  }

}
