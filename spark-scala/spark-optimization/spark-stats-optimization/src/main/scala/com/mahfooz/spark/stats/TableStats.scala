package com.mahfooz.spark.stats

import org.apache.spark.sql.SparkSession

object TableStats {

  def main(args: Array[String]): Unit = {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home",""))

  val spark = SparkSession
    .builder()
    .appName("TableStats")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", sparkWarehouse)
    .enableHiveSupport()
    .getOrCreate()

    import spark.sql

    sql("DESC EXTENDED testdata").show(numRows = 30, truncate = false)

    spark.table("testdata").show

    sql("DESC EXTENDED testdata id").show

  }

}
