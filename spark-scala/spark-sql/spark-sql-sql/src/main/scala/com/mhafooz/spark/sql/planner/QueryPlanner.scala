/*

 */
package com.mhafooz.spark.sql.planner

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object QueryPlanner {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("QueryPlanner")
      .getOrCreate()

    val statesDF = spark.read.option("header", "true")
      .option("inferschema", "true")
      .option("sep", ",")
      .csv("D:\\data\\spark\\csv\\statesPopulation.csv")

    statesDF.printSchema
    statesDF.explain(true)

    statesDF.createOrReplaceTempView("states")
    statesDF.show(5)
    spark.sql("select * from states limit 5").show
    statesDF.sort(col("Population").desc).show(5)
    spark.sql("select * from states order by Population desc limit 5").show
  }
}
