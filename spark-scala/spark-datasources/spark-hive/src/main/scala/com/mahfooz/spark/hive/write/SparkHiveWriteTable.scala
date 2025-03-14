package com.mahfooz.spark.hive.write

import com.mahfooz.spark.hive.common.SparkHiveCommon
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkHiveWriteTable {

  def main(args: Array[String]): Unit = {

   // val warehouseLocation = args(0)
    val tableName = args(0)
    val hiveDestTable=args(1)

    val spark = SparkHiveCommon.createSparkSession("SparkHiveWriteTable")

    val df = spark.table(tableName)

    df.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(hiveDestTable)

    import spark.sql

    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM "+hiveDestTable).show()

    spark.stop()
  }

}
