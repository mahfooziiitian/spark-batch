package com.mahfooz.dataframe.sql

import org.apache.spark.sql.SparkSession

object DataframeToSql {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("DataframeToSql")
      .master("local[*]")
      .getOrCreate()

    val dataFile = s"${sys.env("DATA_HOME")}/FileData/Json/Flight/2015-summary.json"
    val df = spark.read.format("json")
      .load(dataFile)

    df.createOrReplaceTempView("dfTable")

    spark.sql("select * from dfTable").show()

  }
}
