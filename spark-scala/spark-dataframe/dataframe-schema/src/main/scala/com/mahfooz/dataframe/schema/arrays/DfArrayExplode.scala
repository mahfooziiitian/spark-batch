/*
The explode function takes a column that consists of arrays and creates one row (with the rest of the values duplicated) per value in the array.
*/
package com.mahfooz.dataframe.schema.arrays

import com.mahfooz.dataframe.util.download.DownloadDataFile
import com.mahfooz.dataframe.util.unzip.ZipUnzip
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.explode

import java.io.File

object DfArrayExplode {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DfArrayExplode")
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
    SELECT Description, InvoiceNo, exploded
    FROM (SELECT *, split(Description, " ") as splitted FROM dfTable)
    LATERAL VIEW explode(splitted) as exploded
     */
    df.withColumn("split", split(col("Description"), " "))
      .withColumn("exploded", explode(col("split")))
      .select("Description", "InvoiceNo", "exploded")
      .show(10)
  }

}
