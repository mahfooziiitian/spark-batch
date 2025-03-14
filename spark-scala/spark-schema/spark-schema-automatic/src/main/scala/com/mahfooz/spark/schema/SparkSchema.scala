package com.mahfooz.spark.schema

import com.mahfooz.schema.util.{DownloadFile, WindowsToUnixPathConverter}
import org.apache.spark.sql.SparkSession

import java.io.File

object SparkSchema {

  private val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSchema")
      .getOrCreate()

    //downloading data in case it is not downloaded
    val filename = "statesPopulation.csv"
    val downloadFileUrl = "https://raw.githubusercontent.com/mahfooz-code/data/main/statesPopulation.csv"
    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadFile.fileDownload(downloadFileUrl, s"${dataHome}/${filename}")
    }
    val dataPath = WindowsToUnixPathConverter.convertToUnixPath(s"$dataHome/$filename")

    val statesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv(s"${dataPath}")

    statesDF.printSchema

  }
}
