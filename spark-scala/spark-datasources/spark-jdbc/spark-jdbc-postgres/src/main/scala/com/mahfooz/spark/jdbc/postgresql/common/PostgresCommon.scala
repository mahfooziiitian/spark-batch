package com.mahfooz.spark.jdbc.postgresql.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

object PostgresCommon {

  private val LOG= LoggerFactory.getLogger("PostgresCommon")

  def getPostgresCommonProp(): Properties = {
    LOG.warn("started")
    val pgConnectionProperties=new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","postgres")
    LOG.warn("ended")
    pgConnectionProperties
  }

  def getPostgresDbServer(): String = {
    LOG.warn("started")
    val pgUrl="jdbc:postgresql://localhost:5432/postgres"
    LOG.warn("ended")
    pgUrl
  }

  def fetchDataFrameFromPgTable(sparkSession: SparkSession, pgTable: String): DataFrame = {
    LOG.warn("started")
    val jdbcDF=sparkSession.read.jdbc(getPostgresDbServer(),pgTable,getPostgresCommonProp())
    LOG.warn("ended")
    jdbcDF
  }

}
