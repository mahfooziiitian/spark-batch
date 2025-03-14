package com.mahfooz.spark.hive.write

import com.mahfooz.spark.hive.common.SparkHiveCommon

object SparkInsertIntoTable {

  def main(args: Array[String]): Unit = {

    val tableName = args(0)
    val destinationTable = args(1)

    val spark = SparkHiveCommon.createSparkSession("SparkInsertIntoTable")

    val df = spark.table(tableName)

    df.coalesce(1)
      .write
      .format("parquet")
      .insertInto(destinationTable)

    spark.stop()
  }
}
