package com.mahfooz.spark.tsv


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import java.io.File

object SparkTsvReader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val movieSchema = StructType(
      Array(
        StructField("actor", StringType, true),
        StructField("title", StringType, true),
        StructField("year", LongType, true)
      )
    )

    /*
    val filename =  "movies.tsv"
    val downloadFileUrl = "https://raw.githubusercontent.com/mahfooz-code/data/main/movies.tsv"
    if (!new File(s"$dataHome\\$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}\\${filename}")
    }
    val dataPath = "file:///"+FilePathUtil.windowToUnixPath(s"$dataHome\\$filename")
     */

    val dataHome = sys.env.getOrElse("DATA_HOME","data")

    val movies = spark.read
      .option("inferSchema","true")
      .option("header", "true")
      .option("sep", "\t")
      .schema(movieSchema)
      .csv(s"${dataHome}/FileData/Csv/Movies/movies.tsv")

    println(movies.rdd.partitions.size)

    movies.count() //job-1
    //movies.printSchema
    //movies.show(5)

    scala.io.StdIn.readLine()

  }
}
