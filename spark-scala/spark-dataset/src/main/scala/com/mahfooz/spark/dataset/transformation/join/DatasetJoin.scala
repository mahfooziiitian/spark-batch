package com.mahfooz.spark.dataset.transformation.join

import com.mahfooz.spark.dataset.model.{Flight, FlightMetadata}
import org.apache.spark.sql.SparkSession

import scala.util.Random

object DatasetJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val flightsMeta = spark.range(500).map(x => (x, Random.nextLong))
      .withColumnRenamed("_1", "count")
      .withColumnRenamed("_2", "randomData")
      .as[FlightMetadata]

    val flightsDF = spark.read
      .parquet(args(0))

    val flights = flightsDF.as[Flight]

    var flights2 = flights
      .joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))

    flights2.show(10)

    flights2.selectExpr("_1.DEST_COUNTRY_NAME")
    flights2.take(2).foreach(println)

    flights.join(flightsMeta, Seq("count"))
    flights.join(flightsMeta.toDF(), Seq("count"))

  }
}
