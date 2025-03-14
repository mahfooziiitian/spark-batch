package com.mahfooz.spark.dataset.typed.transformation.filter

import com.mahfooz.spark.dataset.model.Flight
import org.apache.spark.sql.SparkSession

object DatasetFilter {

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

    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Parquet/Flight/2010-summary.parquet"

    val flightsDF = spark.read.parquet(dataPath)

    val flights = flightsDF.as[Flight]

    flights.filter(flight => flight.count > 20).show(false)
  }

}
