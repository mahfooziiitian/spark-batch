package com.mahfooz.datafram.aggregation.config

import org.apache.spark.sql.SparkSession

object RetainColumnGroupEx {

  val warehouseLocation = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  private def start(): SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("RollupDF")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark=start()
    println(spark.conf.get("spark.sql.retainGroupColumns"))
    import spark.sessionState.conf
    println(conf.dataFrameRetainGroupColumns)
  }

}
