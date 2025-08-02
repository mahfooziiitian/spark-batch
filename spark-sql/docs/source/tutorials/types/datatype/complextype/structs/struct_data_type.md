package com.mhafooz.spark.sql.datatype.complextype.structs

import org.apache.spark.sql.SparkSession

object StructDataType {

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
      .appName("StructDataType")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      """
        |CREATE VIEW IF NOT EXISTS nested_data AS
        |SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flights
        |""".stripMargin)

    spark.sql("select * from nested_data").show(false)

    spark.sql(
      """
        |SELECT country.DEST_COUNTRY_NAME, count FROM nested_data
        |""".stripMargin).show(false)
  }

}
