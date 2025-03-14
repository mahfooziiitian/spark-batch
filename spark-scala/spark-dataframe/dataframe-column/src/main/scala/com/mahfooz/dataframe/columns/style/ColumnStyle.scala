/*
Way           Example                 Description
""            "columName"             This refers to a column as a string type.
col           col("columnName")       The col function returns an instance of the Column class.
column        column("columnName")    Similar to col, this function returns an instance of the Column class.
$             $"columnName"           This is a syntactic sugar way of constructing a Column class in Scala.
' (tick mark) 'columnName             This is a syntactic sugar way of constructing a Column class in Scala by leveraging the Scala
                                      symbolic literals feature

 */
package com.mahfooz.dataframe.columns.style

import com.mahfooz.dataframe.util.download.DownloadDataFile
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column}

import java.io.File

object ColumnStyle {

  val dataHome = sys.env.get("DATA_HOME").get

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val filename = "2015-summary.json"

    val downloadFileUrl = "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/json/2015-summary.json"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}")
    }


    val df = spark.read
      .format("json")
      .load(s"$dataHome/$filename")

    df.printSchema()

    //"" style
    df.select("DEST_COUNTRY_NAME").show()

    //col style
    df.select(col("ORIGIN_COUNTRY_NAME")).show()

    //column style
    df.select(column("ORIGIN_COUNTRY_NAME")).show()

    //$ style
    import spark.implicits._
    df.select($"count").show()

    //' style
    df.select('count,'count>5).show()

  }

}
