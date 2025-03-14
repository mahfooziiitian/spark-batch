package com.mahfooz.spark.sql.aggregation.group

import com.mahfooz.sql.util.DownloadFile
import com.mahfooz.sql.util.WindowsToUnixPathConverter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File

object CollectionByGroup {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CollectionByGroup")
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

    println(s"number of partition = ${flight_summary.rdd.getNumPartitions}")

    import spark.implicits._

    val highCountDestCities = flight_summary
      .where('count > 5500)
      .groupBy("origin_state")
      .agg(collect_list("dest_city").as("dest_cities"))

    println(s"number partition = ${highCountDestCities.rdd.getNumPartitions}")
    println("df explain")
    highCountDestCities.explain()

    flight_summary.createOrReplaceTempView("flight_summary")

    println("sql explain")
    val highCountDestCitiesSql = spark.sql(
      """select origin_state, collect_list(dest_city) as dest_cities
        |from flight_summary where count>5500
        |group by origin_state""".stripMargin)

    println(s"spark.sql.execution.useObjectHashAggregateExec = " +
      s"${spark.conf.get("spark.sql.execution.useObjectHashAggregateExec")}")

    highCountDestCitiesSql.explain()

    highCountDestCities
      .withColumn("dest_city_count", size('dest_cities))
      .show(5, false)
  }
}
