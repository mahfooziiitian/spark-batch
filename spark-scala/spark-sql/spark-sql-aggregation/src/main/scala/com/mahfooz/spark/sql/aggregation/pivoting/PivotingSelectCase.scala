/*

 */
package com.mahfooz.spark.sql.aggregation.pivoting

import org.apache.spark.sql.SparkSession

object PivotingSelectCase {

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
      .appName("PivotingSelectCase")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("""
        |SELECT
        | CASE  WHEN DEST_COUNTRY_NAME = 'UNITED STATES'
        |         THEN 1
        |       WHEN DEST_COUNTRY_NAME = 'Egypt'
        |         THEN 0
        |       ELSE -1
        | END
        |FROM partitioned_flights
        |""".stripMargin).show(false)

  }

}
