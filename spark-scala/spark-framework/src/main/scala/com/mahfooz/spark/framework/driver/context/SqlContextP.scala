package com.mahfooz.spark.framework.driver.context

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SqlContextP {

  def main(args: Array[String]): Unit = {

    //set up the spark configuration
    val sparkConf = new SparkConf()
      .setAppName("spark-context")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //get SparkContext using the SparkConf
    val sqlContext = new SQLContext(sc)


  }

}
