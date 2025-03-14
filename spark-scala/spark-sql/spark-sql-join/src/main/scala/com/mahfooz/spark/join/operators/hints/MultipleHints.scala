package com.mahfooz.spark.join.operators.hints

import org.apache.spark.sql.SparkSession

object MultipleHints {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("MultipleHints")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val q = spark.range(100).
      hint("broadcast").
      hint("myHint", 100, true)
    val plan = q.queryExecution.logical
    println(plan.numberedTreeString)
    println()

  }

}
