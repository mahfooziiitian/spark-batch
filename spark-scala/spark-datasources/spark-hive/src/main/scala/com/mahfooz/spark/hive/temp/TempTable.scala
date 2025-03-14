package com.mahfooz.spark.hive.temp

import com.mahfooz.spark.hive.model.Record
import org.apache.spark.sql.SparkSession

import java.io.File

object TempTable {

  def main(args: Array[String]): Unit = {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File(sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")).getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("HiveLocal")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

  }

}
