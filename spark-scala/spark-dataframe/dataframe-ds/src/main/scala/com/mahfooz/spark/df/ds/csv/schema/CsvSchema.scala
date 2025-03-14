package com.mahfooz.spark.df.ds.csv.schema

import com.mahfooz.dataframe.util.download.DownloadDataFile
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import java.io.File

object CsvSchema {

  val dataHome = sys.env.get("DATA_HOME").get

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir",s"$dataHome\\spark-warehouse")
      .getOrCreate()

    val myManualSchema = new StructType(
      Array(
        StructField("DEST_COUNTRY_NAME", StringType, true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        StructField("count", LongType, false)
      )
    )

    val filename = "2015-summary.csv"

    val downloadFileUrl = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/csv/2015-summary.csv"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}")
    }


    spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load(s"$dataHome/$filename")
      .show(5)
  }
}
