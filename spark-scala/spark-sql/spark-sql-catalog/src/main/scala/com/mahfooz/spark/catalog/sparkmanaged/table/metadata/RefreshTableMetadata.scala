package com.mahfooz.spark.catalog.sparkmanaged.table.metadata

import org.apache.spark.sql.SparkSession

object RefreshTableMetadata {

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
      .appName("RefreshTableMetadata")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("REFRESH table partitioned_flights")
    spark.sql("show Partitions partitioned_flights").show(false)
    spark.sql("MSCK REPAIR TABLE partitioned_flights")
    spark.sql("show Partitions partitioned_flights").show(false)


  }

}
