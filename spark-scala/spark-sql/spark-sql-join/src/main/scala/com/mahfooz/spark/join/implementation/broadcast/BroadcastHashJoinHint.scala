package com.mahfooz.spark.join.implementation.broadcast

import org.apache.spark.sql.SparkSession

object BroadcastHashJoinHint {

  val sparkWarehouse: String = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("BroadcastHashJoinHint")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    spark.sql("create database if not exists default")
    spark.table("src")
      .join(spark.table("hive_records").hint("broadcast"), "key")
      .explain()

    spark.sql("select /*+ broadcast*/ *  from src s " +
      "inner join hive_records r on s.key=r.key").explain()

  }

}
