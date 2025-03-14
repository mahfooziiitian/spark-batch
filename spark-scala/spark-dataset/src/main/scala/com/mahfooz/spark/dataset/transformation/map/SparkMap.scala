package com.mahfooz.spark.dataset.transformation.map

import com.mahfooz.spark.dataset.model.Flight
import org.apache.spark.sql.SparkSession

object SparkMap {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val flightsDF = spark.read
      .parquet(args(0))

    import spark.implicits._

    val flights = flightsDF.as[Flight]

    val destinations = flights.map(f => f.DEST_COUNTRY_NAME)
    val localDestinations = destinations
      .take(5)
      .foreach(println)
  }
}
