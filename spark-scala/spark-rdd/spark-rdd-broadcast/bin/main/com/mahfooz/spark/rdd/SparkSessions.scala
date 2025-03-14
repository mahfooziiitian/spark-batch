package com.mahfooz.spark.rdd

import org.apache.spark.sql.SparkSession

object SparkSessions {

  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(SparkSessions.getClass.getSimpleName)
      .getOrCreate();

    println("First SparkContext:")
    println("APP Name :"+spark.sparkContext.appName);
    println("Deploy Mode :"+spark.sparkContext.deployMode);
    println("Master :"+spark.sparkContext.master);

    //session will not be overridden
    val sparkSession2 = SparkSession.builder()
      .master("local[1]")
      .appName("SparkSessions-test")
      .getOrCreate();

    println("Second SparkContext:")
    println("APP Name :"+sparkSession2.sparkContext.appName);
    println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
    println("Master :"+sparkSession2.sparkContext.master);

  }
}