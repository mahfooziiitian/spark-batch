package com.mahfooz.spark.hive.common

import org.apache.spark.sql.SparkSession
import java.io.File

object SparkHiveCommon {

  private val warehouseLocation = new File(sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")).getAbsolutePath

  def createSparkSession(appName:String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  def createSparkDynamicSession(appName:String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    spark
  }

  /*def open(spark: SparkSession) = {
    spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
  }*/

}
