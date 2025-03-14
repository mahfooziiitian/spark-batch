package com.mahfoz.elk.search.write

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}
import org.elasticsearch.spark.sql.sparkDatasetFunctions

case class AlbumIndex(artist:String, yearOfRelease:Int, albumName: String)

object ElkSearchWrite {

  def main(args: Array[String]): Unit = {
    ElkSearchWrite.writeToIndex()
  }

  private def writeToIndex(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("WriteToES")
      .master("local[*]")
      .config("es.index.auto.create", "true")
      .config("es.port", "9200")
      .config("es.net.ssl", "false")
      .config("es.nodes", "http://localhost")
      .getOrCreate()

    val dataPath = sys.env.getOrElse("DATA_HOME","data")+"\\FileData\\Parquet\\Movies\\movies.parquet"

    val df = spark.read.parquet(dataPath)

    df.printSchema()

    df.groupBy(col("produced_year")).agg(
      count("*").as("count")
    ).saveToEs("metrics")

  }

}
