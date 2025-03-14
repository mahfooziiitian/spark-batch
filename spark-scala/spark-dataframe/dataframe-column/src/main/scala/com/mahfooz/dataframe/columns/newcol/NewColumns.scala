package com.mahfooz.dataframe.columns.newcol

import com.mahfooz.dataframe.util.download.DownloadDataFile
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, lit}

import java.io.File

object NewColumns {

  val dataHome = sys.env.get("DATA_HOME").get

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(NewColumns.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.warehouse.dir",s"$dataHome\\spark-warehouse")
      .getOrCreate()

    val filename = "2015-summary.json"

    val downloadFileUrl = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/json/2015-summary.json"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val df = spark.read
      .format("json")
      .load(s"${dataHome}/${filename}")

    df.withColumn("numberOne", lit(1))
      .show(2)

    //new column
    df.withColumn("Destination", expr("DEST_COUNTRY_NAME"))
      .columns
      .foreach(println)
    println("--------------------")
    //rename
    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest")
      .columns
      .foreach(println)
  }
}
