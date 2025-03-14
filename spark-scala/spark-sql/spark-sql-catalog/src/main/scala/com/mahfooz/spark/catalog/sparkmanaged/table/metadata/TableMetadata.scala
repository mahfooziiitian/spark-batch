package com.mahfooz.spark.catalog.sparkmanaged.table.metadata

import org.apache.spark.sql.SparkSession

object TableMetadata {

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
      .appName("TableMetadata")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      """
        |DESCRIBE TABLE flights_csv
        |""".stripMargin).show(false)

    spark.sql(
      """
        |SHOW PARTITIONS partitioned_flights
        |""".stripMargin).show(false)
  }

}
