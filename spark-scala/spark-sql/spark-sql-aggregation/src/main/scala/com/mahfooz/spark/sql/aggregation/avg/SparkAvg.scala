package com.mahfooz.spark.sql.aggregation.avg

import com.mahfooz.sql.util.DownloadFile
import com.mahfooz.sql.util.WindowsToUnixPathConverter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, sum}

import java.io.File

object SparkAvg {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkAvg")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val filename = "2015-summary.csv"
    val downloadFileUrl = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/csv/2015-summary.csv"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadFile.fileDownload(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val dataPath = "file:///"+WindowsToUnixPathConverter.convertToUnixPath(s"${dataHome}/${filename}")

    val flight_summary = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(s"${dataPath}")

    val df = flight_summary.select(
      avg("count").as("avg_fun"),
      (sum("count") / count("count")).as("avg")
    )

    df.explain()

    df.show(false)

    spark.stop()
  }
}
