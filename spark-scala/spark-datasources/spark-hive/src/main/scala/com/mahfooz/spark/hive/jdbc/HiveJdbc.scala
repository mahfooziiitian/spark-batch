/*

This is not tested till now and i need to test it.

 */
package com.mahfooz.spark.hive.jdbc

import org.apache.spark.sql.SparkSession

object HiveJdbc {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("HiveJdbc")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:hive2://34.223.237.55:10000")
      .option("dbtable", "students1")
      .option("user", "hdfs")
      .option("fetchsize", "20")
      .load()

    println("able to connect------------------")

    jdbcDF.show

    jdbcDF.printSchema()

    jdbcDF.createOrReplaceTempView("std")

    val sqlDF = spark.sql("select * from std")
    println("Start println-----")

    spark.sqlContext.sql("select * from std").collect().foreach(println)
    println("end println-----")

    sqlDF.show(false)
  }
}
