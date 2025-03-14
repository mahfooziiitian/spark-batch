package com.mahfooz.spark.application.driver.context

import org.apache.spark.SparkContext

object SparkContextPattern {
  def main(args: Array[String]): Unit = {

    val sc = SparkContext.getOrCreate()


  }
}
