package com.mahfooz.spark.jdbc.postgresql.common

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkCommon {

  private val LOG= LoggerFactory.getLogger("SparkCommon")

  def createSparkSession(appName: String): SparkSession = {
    LOG.warn("started")
    val sparkSession = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
    LOG.warn("ended")
    sparkSession
  }
}
