package com.mahfooz.dataframe.columns.newcol

import com.mahfooz.dataframe.util.download.DownloadDataFile
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

import java.io.File

object NewColumnBoolean {

  def main(args: Array[String]): Unit = {

    val dataHome = sys.env.get("DATA_HOME").get

    val spark = SparkSession.builder
      .appName(NewColumnBoolean.getClass.getSimpleName)
      .master("local")
      .config(
        "spark.sql.warehouse.dir",
        s"$dataHome\\spark-warehouse"
      )
      .getOrCreate()


    val filename = "2015-summary.json"

    val downloadFileUrl = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/json/2015-summary.json"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val df = spark.read
      .format("json")
      .load(s"$dataHome/$filename")

    // in Scala
    df.withColumn(
        "withinCountry",
        expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")
      )
      .show(2)

  }
}
