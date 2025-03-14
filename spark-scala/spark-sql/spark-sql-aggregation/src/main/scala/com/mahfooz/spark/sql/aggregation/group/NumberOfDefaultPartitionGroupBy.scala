package com.mahfooz.spark.sql.aggregation.group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object NumberOfDefaultPartitionGroupBy {

  // The goal of the case study is to fine tune
  // the number of partitions used for groupBy aggregation.

  /**
   * @param args
   * Given the following 2-partition dataset the task is
   * to write a structured query so there are no empty partitions
   */
  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
  val dataHome = sys.env.getOrElse("DATA_HOME","data")+"\\spark\\datasource\\csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("NumberOfDefaultPartitionGroupBy")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val ids = spark.range(start = 0, end = 4, step = 1, numPartitions = 2)
    ids.show
    println(s"number_parts = ${ids.rdd.getNumPartitions}")
    println(ids.rdd.toDebugString)

    //By default Spark SQL uses spark.sql.shuffle.partitions
    // number of partitions for aggregations and joins, i.e. 200 by default.
    //Case 1: Default Number of Partitions -> spark.sql.shuffle.partitions Property
    import spark.implicits._

    val groupingExpr = 'id % 2 as "group"

    val q = ids.
      groupBy(groupingExpr).
      agg(count($"id") as "count")

    //You may have expected to have at most 2 partitions given the number of groups.
    //Wrong!
    q.explain

    println(q.rdd.toDebugString)

  }

}
