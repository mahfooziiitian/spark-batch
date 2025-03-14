package com.mahfooz.spark.rdd.dataframe.common

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkCommon {

  private val LOG = LoggerFactory.getLogger(SparkCommon.getClass.getName())

  def createSparkSession(appName: String): SparkSession = {
    LOG.warn("createSparkSession method started")
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()
    LOG.warn("createSparkSession method ended")
    sparkSession
  }
}
