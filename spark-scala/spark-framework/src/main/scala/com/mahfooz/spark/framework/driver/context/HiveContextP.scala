package com.mahfooz.spark.framework.driver.context

import org.apache.spark.{SparkConf, SparkContext}

object HiveContextP {

  def main(args: Array[String]): Unit = {

    //set up the spark configuration
    val sparkConf = new SparkConf()
      .setAppName("hive-context")
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)


    // sc is an existing SparkContext.
    val sqlContext = new HiveContext(sc)
  }

}
