package com.mahfooz.spark.adls.adls.key

import org.apache.spark.sql.SparkSession

object SparkAzureAdlsKey {

  def main(args: Array[String]): Unit = {

    val sparkWarehouseDirectory = s"file:///${sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse").replace("\\","/")}"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir",sparkWarehouseDirectory)
      .getOrCreate()

    val storageAccountName =
      sys.env.getOrElse("STORAGE_ACCOUNT_NAME", "storageAccount")
    val storageAccountAccessKey =
      sys.env.getOrElse("ACCESS_KEY", "accessKey")

    val file_location = s"abfss://data@${storageAccountName}.dfs.core.windows.net/csv/population.csv"
    println(file_location)
    val file_type = "csv"

    spark.conf.set(
      "fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net",
      storageAccountAccessKey
    )

    val df = spark.read.format(file_type)
      .option("inferSchema", "true")
      .option("header",true)
      .load(file_location)

    df.show(false)

  }
}
