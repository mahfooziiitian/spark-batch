package com.mahfooz.spark.dag

import org.apache.spark.sql.SparkSession

object SparkDagSimple {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .getOrCreate()

    val df = spark.range(1,100000)
    val df5Times = df.selectExpr("id*5 as id")


  }
}
