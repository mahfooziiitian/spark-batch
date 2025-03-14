package com.mahfooz.spark.dml.table

import org.apache.spark.sql.SparkSession

object InsertIntoPartitionTable {

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
      .appName("InsertIntoPartitionTable")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("truncate table partitioned_flights")

    spark.sql(
      """
        |INSERT INTO partitioned_flights
        |PARTITION (DEST_COUNTRY_NAME='United States')
        |SELECT count, ORIGIN_COUNTRY_NAME FROM flights
        |WHERE DEST_COUNTRY_NAME='United States' LIMIT 12
        |""".stripMargin)

    spark.sql("select * from partitioned_flights").show(false)
  }

}
