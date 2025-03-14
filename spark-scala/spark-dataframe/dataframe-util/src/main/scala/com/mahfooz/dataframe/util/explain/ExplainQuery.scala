package com.mahfooz.dataframe.util.explain

import org.apache.spark.sql.SparkSession

object ExplainQuery {

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

    spark.sql("EXPLAIN SELECT * FROM flights WHERE dest_country_name = 'United States'").show(false)
  }

}
