package com.mahfooz.datafram.aggregation.groupby

import com.mahfooz.dataframe.util.download.DownloadDataFile
import com.mahfooz.dataframe.util.path.FilePathUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File

object MultiAggregationPerGroup {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MultiAggregationPerGroup")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val filename = "flight-summary.csv"
    val downloadFileUrl = "https://raw.githubusercontent.com/Apress/beginning-apache-spark-2/master/chapter5/data/flights/flight-summary.csv"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val dataPath = "file:///"+FilePathUtil.windowToUnixPath(s"${dataHome}/${filename}")

    val flight_summary = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"${dataPath}")

    val flightAggDF = flight_summary.groupBy("origin_airport")
      .agg(
        count("count").as("count"),
        min("count"), max("count"),
        sum("count")
      )

    println(flightAggDF.queryExecution.logical.numberedTreeString)
  }

}
