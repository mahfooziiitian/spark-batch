package com.mahfooz.spark.jdbc.write

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteSparkJdbc {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("WriteSparkJdbc")
      .getOrCreate()

    //Reading from jdbc.
    val jdbcDF = spark.read
      .format("json")
      .option("dateFormat", "YYYY-MM-dd")
      .option("inferSchema", "true")
      .option("multiline", true)
      .load(args(0))

    jdbcDF.printSchema()

    // Saving data to a JDBC source
    jdbcDF.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:postgresql://192.168.0.101:5432/postgres")
      .option("dbtable", "films")
      .option("user", "postgres")
      .option("password", "postgres")
      .save()
  }
}
