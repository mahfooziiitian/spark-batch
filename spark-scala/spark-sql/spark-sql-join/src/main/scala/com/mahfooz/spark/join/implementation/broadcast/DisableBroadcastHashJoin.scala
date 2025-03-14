package com.mahfooz.spark.join.implementation.broadcast

import org.apache.spark.sql.SparkSession

object DisableBroadcastHashJoin {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("DisableBroadcastHashJoin")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    import spark.implicits._

    val q = spark.range(100).as("a").join(spark.range(100).as("b"))
      .where($"a.id" === $"b.id")

    println(q.queryExecution.logical.numberedTreeString)

  }

}
