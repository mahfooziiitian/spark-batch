/**
 */
package com.mahfooz.spark.framework.driver.context

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextP {

  def main(args: Array[String]): Unit = {
    //set up the spark configuration
    val sparkConf = new SparkConf().setAppName("spark-context").setMaster("local[*]")

    //get SparkContext using the SparkConf
    val sc = new SparkContext(sparkConf)
    
  }

}
