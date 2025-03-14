package com.mahfooz.spark.jdbc.postgresql.read

import com.mahfooz.spark.jdbc.postgresql.common.{PostgresCommon, SparkCommon}
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

object ReadJdbcToCsvFile {

  private val LOG= LoggerFactory.getLogger("ReadJdbcToCsvFile")

  def main(args: Array[String]): Unit = {

    //1. Creating spark session
    val sparkSession=SparkCommon.createSparkSession("ReadJdbcToCsvFile")

    //2. Fetching the data from database
    val jdbcDF = PostgresCommon.fetchDataFrameFromPgTable(sparkSession,"persons")
    jdbcDF.show(20)

    //3. Saving file into CSV file
    jdbcDF.write
      .mode(SaveMode.Overwrite)
      .csv("d:/data/spark/csv/person.csv")
  }
}
