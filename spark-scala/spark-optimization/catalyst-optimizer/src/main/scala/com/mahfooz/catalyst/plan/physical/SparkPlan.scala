package com.mahfooz.catalyst.plan.physical

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object SparkPlan {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("caching-cost")
      .master("local[*]")
      .getOrCreate()

    val dataframe = spark.range(1,10000000).toDF("id")
      .withColumn("odd_even",expr("id%2"))

    val df = dataframe.groupBy("odd_even").count()

    println(s"spark plan \n ${df.queryExecution.sparkPlan}")
    println(s"executed plan \n ${df.queryExecution.executedPlan}")

  }

}
