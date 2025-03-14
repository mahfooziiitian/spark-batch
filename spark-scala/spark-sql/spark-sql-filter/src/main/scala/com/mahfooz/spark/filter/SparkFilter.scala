package com.mahfooz.spark.filter

import org.apache.spark.sql.SparkSession

object SparkFilter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkFilter")
      .getOrCreate()

    val statesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv("D:\\data\\processing\\batch\\spark\\csv\\statesPopulation.csv")

    statesDF.createOrReplaceTempView("states_df")

    spark.sql("select * from states_df where state='California'").explain(true)

    spark.sql("select * from states_df where state='California'").show
  }
}
