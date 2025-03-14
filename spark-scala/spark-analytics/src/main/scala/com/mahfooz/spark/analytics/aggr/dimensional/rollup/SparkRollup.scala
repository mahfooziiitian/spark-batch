package com.mahfooz.spark.analytics.aggr.dimensional.rollup

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkRollup {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME","flights")

    // read in the flight summary data
    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"${dataHome}/FileData/Csv/flight-summary.csv")

    import spark.implicits._

    // filter data down to smaller size to make it easier to see the rollup result
    val twoStatesSummary = flight_summary
      .select('origin_state, 'origin_city, 'count)
      .where('origin_state === "CA" || 'origin_state === "NY")
      .where('count > 1 && 'count < 20)
      .where('origin_city =!= "White Plains")
      .where('origin_city =!= "Newburgh")
      .where('origin_city =!= "Mammoth Lakes")
      .where('origin_city =!= "Ontario")

    // perform the rollup by state, city, then calculate the sum of the count,
    // and finally order by null last
    twoStatesSummary
      .rollup('origin_state, 'origin_city)
      .agg(sum("count") as "total")
      .orderBy('origin_state.asc_nulls_last, 'origin_city.asc_nulls_last)
      .show

  }
}
