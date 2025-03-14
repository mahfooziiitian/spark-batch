package com.mahfooz.spark.catalog.sparkmanaged.table.partition

import org.apache.spark.sql.SparkSession

object PartitionManagedTable {

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
      .appName("PartitionManagedTable")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      """
        |CREATE TABLE partitioned_flights USING parquet PARTITIONED BY(DEST_COUNTRY_NAME)
        |    AS SELECT DEST_COUNTRY_NAME
        |    , ORIGIN_COUNTRY_NAME
        |    , count FROM flights LIMIT 5
        |""".stripMargin)

    spark.sql("select * from partitioned_flights").show(false)

  }

}
