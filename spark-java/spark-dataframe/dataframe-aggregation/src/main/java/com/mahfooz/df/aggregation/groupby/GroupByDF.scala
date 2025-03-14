package com.mahfooz.df.aggregation.groupby

import com.mahfooz.dataframe.util.download.DownloadDataFile
import com.mahfooz.dataframe.util.path.FilePathUtil
import org.apache.spark.sql.SparkSession

import java.io.File

object GroupByDF {

  val warehouseLocation = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("MaxMin")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = start()

    val filename = "statesPopulation.csv"
    val downloadFileUrl = "https://raw.githubusercontent.com/PacktPublishing/Scala-and-Spark-for-Big-Data-Analytics/master/data/data/statesPopulation.csv"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}")
    }

    val dataPath = "file:///"+FilePathUtil.windowToUnixPath(s"${dataHome}/${filename}")

    val statesPopulationDF =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(s"${dataPath}")

    println(statesPopulationDF.rdd.getNumPartitions)

    val groupStatePopulationDf=statesPopulationDF.groupBy("State").count()

    println(groupStatePopulationDf.rdd.getNumPartitions)

    groupStatePopulationDf.show()

    scala.io.StdIn.readLine()
  }

}
