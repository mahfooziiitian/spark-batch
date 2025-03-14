package com.mahfooz.spark.dml.table

import org.apache.spark.sql.SparkSession

object InsertIntoTable {

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
      .appName("InsertIntoTable")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("truncate table flights_from_select")

    spark.sql(
      """
        |INSERT INTO flights_from_select
        |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20
        |""".stripMargin)

    spark.sql("select * from flights_from_select").show(false)

  }

}
