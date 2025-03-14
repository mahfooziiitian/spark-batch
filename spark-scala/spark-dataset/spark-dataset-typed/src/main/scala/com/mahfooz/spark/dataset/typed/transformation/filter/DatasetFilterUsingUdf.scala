package com.mahfooz.spark.dataset.typed.transformation.filter

import com.mahfooz.spark.dataset.model.Flight
import org.apache.spark.sql.SparkSession

object DatasetFilterUsingUdf {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DatasetFilter")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    val dataPath = s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Parquet/Flight/2010-summary.parquet"

    val flightsDF = spark.read
      .parquet(dataPath)

    val flights = flightsDF.as[Flight]

    println(flights.filter(flight_row => originIsDestination(flight_row)).first())
  }

  private def originIsDestination(flight_row: Flight): Boolean = {
    flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
  }

}
