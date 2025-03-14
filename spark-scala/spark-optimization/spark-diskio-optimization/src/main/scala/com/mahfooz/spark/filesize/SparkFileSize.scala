/*
You can use the maxRecordsPerFile option and specify a number of your choosing.

This allows you to better control file sizes by controlling the number of records that are
written to each file.

For example, if you set an option for a writer as df.write.option("maxRecordsPerFile", 5000).

 */
package com.mahfooz.spark.filesize

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkFileSize {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SparkFileSize")
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME","data")+"/FileData/Csv/Movies"

    val csvFile = spark.read.format("csv")
      .option("inferSchema",true)
      .option("header",true)
      .load(s"${dataHome}/movies.csv")


    csvFile.write
      .mode(SaveMode.Overwrite)
      .option("maxRecordsPerFile", 5000)
      .save(s"${dataHome}/FileSize")

  }

}
