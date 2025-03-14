package com.mahfooz.spark.analytics.aggr.dimensional.groupset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{exp, expr, grouping_id, sum}

object GroupSetMetadata {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkCube")
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME", "flights")

    // read in the flight summary data
    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"${dataHome}/Processing/Spark/DataSources/Csv/flight-summary.csv")

    import spark.implicits._

    // filter data down to smaller size to make it easier to see the roll-ups result
    val twoStatesSummary = flight_summary
      .select('origin_state, 'origin_city, 'count)
      .where('origin_state === "CA" || 'origin_state === "NY")
      .where('count > 1 && 'count < 20)
      .where('origin_city =!= "White Plains")
      .where('origin_city =!= "Newburgh")
      .where('origin_city =!= "Mammoth Lakes")
      .where('origin_city =!= "Ontario")

    // perform the cube across origin_state and origin_city
    twoStatesSummary
      .cube('origin_state, 'origin_city)
      .agg(
        grouping_id(),
        sum("count") as "total")
      .orderBy(expr("grouping_id()").desc)
      .show()
  }

}
