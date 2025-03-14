/*

Maps are created by using the map function and key-value pairs of columns.

 */
package com.mahfooz.dataframe.schema.complextype

import com.mahfooz.dataframe.util.download.DownloadDataFile
import com.mahfooz.dataframe.util.unzip.ZipUnzip
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, map}

import java.io.File

object DfMapType {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DfMapType")
      .getOrCreate()

    val dataHome = sys.env.get("DATA_HOME").get
    val filename = "OnlineRetail.csv"

    val downloadFileUrl = "https://storage.googleapis.com/kaggle-data-sets/3466/5596/compressed/OnlineRetail.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20220719%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20220719T181215Z&X-Goog-Expires=259199&X-Goog-SignedHeaders=host&X-Goog-Signature=890345b6140b4ed8423af8420b13c3d8b334d2a31cfeeaa400a6755c0dac1043ce2c2405823d51208caf6bc5c9520bf1235d1663fe75a327dc8a9936687c9dd8a5e79ad6d6451d56461f5f34f10461c65b71e6d3e4ba066309a38a98bd0ee94d96cea8571d3e7d0cb6a3803b9e20f7abd68bf01f3237a4c68d6d575218745071a709e8acb236e0189f25eaaddef60db074e834761af2c8495d4714b0d6f3a18bc633b8271be033852ce0a4526ed572660272eb541f714b7b4d9164f49eedeed65ec4ca6c25b71a0d0b0ae4fceeeee28e07a64dc31d51d6e496c9577f875115b42dbe0d7744f97bca4e645d55ffe2a6ececfd4cc8998a5398eeee6d2f874de553"

    if (!new File(s"$dataHome/$filename").exists()) {
      DownloadDataFile.fileDownloader(downloadFileUrl, s"${dataHome}/${filename}.zip")
      ZipUnzip.unzip(s"${dataHome}/${filename}.zip",dataHome)
    }

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"$dataHome/$filename")

    /*
    SELECT map(Description, InvoiceNo) as complex_map FROM dfTable
    WHERE Description IS NOT NULL
     */
    df.select(
        map(col("Description"), col("InvoiceNo"))
          .alias("complex_map")
      )
      .show(2, truncate = false)

    df.select(
        map(col("Description"), col("InvoiceNo"))
          .alias("complex_map")
      )
      .selectExpr("complex_map['WHITE METAL LANTERN']")
      .show(2, truncate = false)

    df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
      .selectExpr("explode(complex_map)")
      .show(2)

  }

}
