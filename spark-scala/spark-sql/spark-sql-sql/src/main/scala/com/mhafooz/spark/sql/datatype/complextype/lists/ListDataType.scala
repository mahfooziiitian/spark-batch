package com.mhafooz.spark.sql.datatype.complextype.lists

import org.apache.spark.sql.SparkSession

object ListDataType {

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
        |SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts,
        |collect_set(ORIGIN_COUNTRY_NAME) as origin_set
        |FROM flights
        |GROUP BY DEST_COUNTRY_NAME having size(collect_list(count))>1
        |""".stripMargin).show(false)

  }

}
