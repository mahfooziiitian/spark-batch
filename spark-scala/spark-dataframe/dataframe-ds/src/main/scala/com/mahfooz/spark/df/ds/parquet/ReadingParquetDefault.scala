package com.mahfooz.spark.df.ds.parquet

import com.mahfooz.dataframe.util.download.DownloadDataFile
import org.apache.spark.sql.SparkSession

import java.io.File

object ReadingParquetDefault {

  def main(args: Array[String]): Unit = {

    val dataHome = sys.env.get("DATA_HOME").get

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val filename = "part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet"

    val downloadFileUrl = "https://github.com/databricks/Spark-The-Definitive-Guide/blob/master/data/flight-data/parquet/2010-summary.parquet/part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet"

    if (!new File(s"$dataHome/2010-summary.parquet/$filename").exists()
      && new File(s"$dataHome/2010-summary.parquet/").mkdir()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/2010-summary.parquet/${filename}")
    }


    spark.read
      .load(s"$dataHome\\2010-summary.parquet")
      .show(5)
  }
}
