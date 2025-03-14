package com.mahfooz.spark.types.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf,col}

object UdfFunction {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("UdfFunction")
      .master("local")
      .getOrCreate()

    val udfExampleDF = spark.range(5).toDF("num")

    def power3(number:Double):Double = number * number * number

    println(power3(2.0))
    val power3udf = udf(power3(_:Double):Double)
    udfExampleDF.select(power3udf(col("num"))).show()

    spark.udf.register("power3", power3(_:Double):Double)
    udfExampleDF.selectExpr("power3(num)").show(2)

  }
}
