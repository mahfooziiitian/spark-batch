package com.mahfooz.spark.join.operators.hints

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object SparkSqlHintOperator {

  val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("SparkSqlHintOperator")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .getOrCreate()

    val small = spark.range(1)
    val large = spark.range(100)

    val q = large.join(broadcast(small), "id")
    val plan = q.queryExecution.logical
    println(plan.numberedTreeString)
    println()

    //sql hint
    small.createOrReplaceTempView("small")
    large.createOrReplaceTempView("large")

    val joinQuery = spark.sql(
      """select /*+broadcast(small)*/ * from small s
        |inner join large l on s.id = l.id""".stripMargin)
    val planSql = joinQuery.queryExecution.logical
    println(planSql.numberedTreeString)
    println()

    val smallHinted = small.hint("broadcast")
    val planHint = smallHinted.queryExecution.logical
    println(planHint.numberedTreeString)
    println()

    val queryHint = large.join(smallHinted, "id")
    val planHintJoin = queryHint.queryExecution.logical
    println(planHintJoin.numberedTreeString)

  }

}
