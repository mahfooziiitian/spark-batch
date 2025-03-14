package com.mahfooz.dataframe.transformation.projection.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, lit, to_date, to_timestamp}

object DfDateFormat {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DfDateFormat")
      .getOrCreate()

    val dateFormat = "yyyy-dd-MM"
    /*
    SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date)
    FROM dateTable
     */
    val cleanDateDF = spark.range(1).select(
      to_date(lit("2017-12-11"), dateFormat).alias("date"),
      to_date(lit("2017-20-12"), dateFormat).alias("date2"))

    cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

    /*
    SELECT to_timestamp(date, 'yyyy-dd-MM'), to_timestamp(date2, 'yyyy-dd-MM')
    FROM dateTable
    SELECT cast(to_date("2017-01-01", "yyyy-dd-MM") as timestamp)
     */
    cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()



  }

}
