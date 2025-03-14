package com.mahfooz.spark.catalog.function

import org.apache.spark.sql.SparkSession

object FunctionCatalog {

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
      .appName("FunctionCatalog")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    //Accessing Catalog
    val catalog = spark.catalog

    catalog.listFunctions().
      select("name", "description", "className", "isTemporary").show(100)

  }

}
