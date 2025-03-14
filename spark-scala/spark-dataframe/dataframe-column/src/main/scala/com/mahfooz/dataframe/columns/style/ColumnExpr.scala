package com.mahfooz.dataframe.columns.style

import com.mahfooz.dataframe.util.download.DownloadDataFile
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File

object ColumnExpr {

  val dataHome = sys.env.get("DATA_HOME").get

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val filename = "2015-summary.json"

    val downloadFileUrl = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/json/2015-summary.json"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val df = spark.read
      .format("json")
      .load(s"$dataHome/$filename")

    df.printSchema()
    df.select(expr("nvl(DEST_COUNTRY_NAME, 'unknown') as renamed"))
      .show(false)
  }
}
