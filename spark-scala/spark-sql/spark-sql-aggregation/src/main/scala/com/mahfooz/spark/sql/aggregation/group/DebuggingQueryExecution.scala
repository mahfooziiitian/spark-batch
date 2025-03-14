package com.mahfooz.spark.sql.aggregation.group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.debug.DebugQuery

object DebuggingQueryExecution {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("NumberOfCustomPartitionGroupBy")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    import spark.implicits._

    spark.range(10).where('id === 4).debugCodegen

  }

}
