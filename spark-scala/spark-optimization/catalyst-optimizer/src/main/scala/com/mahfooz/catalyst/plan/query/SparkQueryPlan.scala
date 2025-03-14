package com.mahfooz.catalyst.plan.query

import org.apache.spark.sql.SparkSession

object SparkQueryPlan {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("NumberOfCustomPartitionGroupBy")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val dataset = spark.range(3)

    println(dataset.queryExecution.analyzed.schema)
    println(dataset.queryExecution.withCachedData.output)
    println(dataset.queryExecution.optimizedPlan.output)
    println(dataset.queryExecution.sparkPlan.output)
    println(dataset.queryExecution.executedPlan.output)

  }

}
