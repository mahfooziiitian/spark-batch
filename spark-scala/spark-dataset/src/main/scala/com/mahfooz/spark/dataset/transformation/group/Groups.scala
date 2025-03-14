package com.mahfooz.spark.dataset.transformation.group

import com.mahfooz.spark.dataset.model.Flight
import org.apache.spark.sql.SparkSession

object Groups {

  def grpSum(countryName:String, values: Iterator[Flight]) = {
    values.dropWhile(_.count < 5).map(x => (countryName, x))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val flightsDF = spark.read
      .parquet(args(0))

    val flights = flightsDF.as[Flight]

    flights.groupBy("DEST_COUNTRY_NAME").count().show()
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().show()
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)
  }
}
