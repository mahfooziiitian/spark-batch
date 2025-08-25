package com.mahfooz.spark.sql.aggregation.count

import com.mahfooz.sql.util.DownloadFile
import com.mahfooz.sql.util.WindowsToUnixPathConverter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File

object ApproxCountDistinct {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ApproxCountDistinct")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val filename = "flight-summary.csv"
    val downloadFileUrl = "https://raw.githubusercontent.com/Apress/beginning-apache-spark-2/master/chapter5/data/flights/flight-summary.csv"

   if (!new File(s"$dataHome/$filename").exists()) {
      DownloadFile.fileDownload(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val dataPath = "file:///"+WindowsToUnixPathConverter.convertToUnixPath(s"${dataHome}/${filename}")

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"${dataPath}")

    flight_summary
      .select(
        countDistinct("origin_airport"),
        countDistinct("dest_airport"),
        count("*")
      )
      .show

    val df = flight_summary
      .select(
        count("count"),
        countDistinct("count"),
        approx_count_distinct("count", 0.01)
      )

    df.explain()

    df.show(false)
  }
}
