package com.mahfooz.spark.sql.aggregation.group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object NumberOfCustomPartitionGroupBy {

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
      .appName("NumberOfCustomPartitionGroupBy")
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
      repartition(groupingExpr). // <-- repartition per groupBy expression
      groupBy(groupingExpr).
      agg(count($"id") as "count")

    //You may have expected 2 partitions again?!
    //Wrong!
    q.explain

    println(q.rdd.toDebugString)

    //Case 3: Using repartition Operator With Explicit Number of Partitions
    //repartition(numPartitions: Int, partitionExprs: Column*): Dataset[T]
    val q3 = ids.
      repartition(numPartitions = 2, groupingExpr). // <-- repartition per groupBy expression
      groupBy(groupingExpr).
      agg(count($"id") as "count")

    //Congratulations! You are done.
    //Not quite.
    q3.explain()

    println(q3.rdd.toDebugString)
    println(s"number_parts = ${q3.rdd.getNumPartitions}")

    //Case 4: Remember spark.sql.shuffle.partitions Property? Set It Up Properly
    import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, 2)
    // spark.conf.set(SHUFFLE_PARTITIONS.key, 2)

    println(s"spark.sessionState.conf.numShufflePartitions = ${spark.sessionState.conf.numShufflePartitions}")

    val q4 = ids.
      groupBy(groupingExpr).
      agg(count($"id") as "count")

    q4.explain

    println(q4.rdd.toDebugString)
    //The number of Succeeded Jobs is 2.
    //Congratulations! You are done now.

  }

}
