/*
Rollup is a multi-dimensional aggregation used to perform hierarchical or nested calculations.
For example, if we want to show the number of records for each State+Year group, as well as for each State
(aggregating over all years to give a grand total for each State irrespective of the Year), we can use rollup as
follows:

 */
package com.mahfooz.datafram.aggregation.rollup

import com.mahfooz.dataframe.util.download.DownloadDataFile
import com.mahfooz.dataframe.util.path.FilePathUtil
import org.apache.spark.sql.SparkSession

import java.io.File

object RollupDF {

  val warehouseLocation = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("RollupDF")
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
        .option("inferschema", "true")
        .csv(s"${dataPath}")

    import spark.implicits._

    val rollupStateYear = statesPopulationDF
      .where($"state"==="South Dakota")
      .rollup("State", "Year").count

    println(rollupStateYear.queryExecution.toRdd.toDebugString)
    rollupStateYear.explain(true)

    rollupStateYear.show()

    println(rollupStateYear.queryExecution.logical.numberedTreeString)
  }
}
