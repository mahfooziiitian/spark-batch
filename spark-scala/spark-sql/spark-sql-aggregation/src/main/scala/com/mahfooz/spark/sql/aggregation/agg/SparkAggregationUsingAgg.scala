package com.mahfooz.spark.sql.aggregation.agg

import org.apache.spark.sql.SparkSession

import java.io.File
import com.mahfooz.sql.util.DownloadFile
import com.mahfooz.sql.util.WindowsToUnixPathConverter

object SparkAggregationUsingAgg {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkAggregationUsingAgg")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val filename = "statesPopulation.csv"

    val downloadFileUrl = "https://raw.githubusercontent.com/PacktPublishing/Scala-and-Spark-for-Big-Data-Analytics/master/data/data/statesPopulation.csv"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadFile.fileDownload(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val dataPath = "file:///"+WindowsToUnixPathConverter.convertToUnixPath(s"${dataHome}/${filename}")

    val statesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv(s"${dataPath}")

    statesDF.createOrReplaceTempView("states")

    spark.sql("select state,min(Population) as minTotal," +
      "max(Population) as maxTotal," +
      "avg(Population) as avgTotal from states group by state " +
      " order by minTotal desc " +
      "limit 5")
      .show()
  }

}
