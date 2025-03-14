/*

 */
package com.mahfooz.sparksql.generic.sql

import org.apache.spark.sql.SparkSession

object SparkSqlQuery {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    var fileName = "D:\\data\\flight-data\\csv\\2015-summary.csv"
    if (args.length > 0) {
      fileName = args(0)
    }

    val flightData2015 = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(fileName)

    flightData2015.createOrReplaceTempView("flight_data_2015")

    val maxSql =
      spark.sql("""
        SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 5""")
    maxSql.show()
  }
}
