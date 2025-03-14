package com.mahfooz.schema.data

import com.mahfooz.schema.util.DownloadFile
import com.mahfooz.schema.util.WindowsToUnixPathConverter
import org.apache.spark.sql.SparkSession

import java.io.File

object SchemaFromData {

  private val dataHome =
    sys.env.getOrElse("DATA_HOME", "data").replace("\\","/") + "/FileData/Json"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SchemaFromData")
      .getOrCreate()

    //downloading data in case it is not downloaded
    val filename = "2015-summary.json"
    val downloadFileUrl =
      "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/flight-data/json/2015-summary.json"
    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadFile.fileDownload(
        downloadFileUrl,
        s"${dataHome}/${filename}"
      )
    }
    print(s"$dataHome/$filename")
    val dataPath = WindowsToUnixPathConverter.convertToUnixPath(s"$dataHome/$filename")

    print(dataPath)

    val df = spark.read
      .format("json")
      .load(s"${dataPath}")
    df.printSchema()

  }

}
