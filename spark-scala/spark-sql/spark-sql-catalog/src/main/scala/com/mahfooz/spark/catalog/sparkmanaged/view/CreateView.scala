package com.mahfooz.spark.catalog.sparkmanaged.view

import org.apache.spark.sql.SparkSession

object CreateView {

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
      .appName("CreateView")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      """
        |CREATE VIEW if not exists just_usa_view AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        |""".stripMargin)

    spark.sql("select * from just_usa_view").show(false)

    //Creating temporary view
    spark.sql(
      """
        |CREATE TEMP VIEW just_usa_view_temp AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        |""".stripMargin)

    spark.sql(
      """
        |select * from just_usa_view_temp
        |""".stripMargin).show(false)

    //Temporary global view
    spark.sql(
      """
        |CREATE GLOBAL TEMP VIEW  just_usa_global_view_temp AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        |""".stripMargin)

    spark.sql(
     """
        |select * from global_temp.just_usa_global_view_temp
        |""".stripMargin).show(false)

    spark.sql(
      """
        |SHOW TABLES
        |""".stripMargin).show(false)

  }

}
