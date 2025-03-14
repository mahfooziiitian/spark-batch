package com.mahfooz.dataset.join

import com.mahfooz.spark.dataset.model.{Flight, FlightMetadata}
import org.apache.spark.sql.SparkSession

object DatasetJoin {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkHiveDatabase")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    val flightsMeta = spark
      .range(500)
      .map(x => (x, scala.util.Random.nextLong))
      .withColumnRenamed("_1", "count")
      .withColumnRenamed("_2", "randomData")
      .as[FlightMetadata]

    flightsMeta.show(false)

    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Parquet/Flight/2010-summary.parquet"

    val flights = spark.read
      .parquet(dataPath).as[Flight]

    val flights2 = flights
      .joinWith(flightsMeta,
        flights.col("count") === flightsMeta.col("count"))

    flights2.show(false)

  }

}
