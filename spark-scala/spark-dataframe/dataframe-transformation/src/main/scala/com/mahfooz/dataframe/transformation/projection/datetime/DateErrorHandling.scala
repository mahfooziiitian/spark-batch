/*

Spark will not throw an error if it cannot parse the date; rather, it will just return null.
This can be a bit tricky in larger pipelines because you might be expecting your data in one format and getting
it in another.

 */
package com.mahfooz.dataframe.transformation.projection.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, current_timestamp, lit, to_date}

object DateErrorHandling {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DateErrorHandling")
      .getOrCreate()

    val dateDF = spark
      .range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())

    dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

    //fixed it by date format

  }

}
