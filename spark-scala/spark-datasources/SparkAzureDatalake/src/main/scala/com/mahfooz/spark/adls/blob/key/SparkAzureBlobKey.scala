package com.mahfooz.spark.adls.blob.key

import org.apache.spark.sql.SparkSession

object SparkAzureBlobKey {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val storageAccountName =
        sys.env.getOrElse("STORAGE_ACCOUNT_NAME", "storageAccount")
    val storageAccountAccessKey =
      sys.env.getOrElse("ACCESS_KEY", "accessKey")

    val file_location = s"wasbs://data@${storageAccountName}.blob.core.windows.net/csv/population.csv"
    println(file_location)
    val file_type = "csv"

    spark.conf.set(
      "fs.azure.account.key." + storageAccountName + ".blob.core.windows.net",
      storageAccountAccessKey
    )

    val df = spark.read.format(file_type)
      .option("inferSchema", "true")
      .load(file_location)

    df.show(false)

  }

}
