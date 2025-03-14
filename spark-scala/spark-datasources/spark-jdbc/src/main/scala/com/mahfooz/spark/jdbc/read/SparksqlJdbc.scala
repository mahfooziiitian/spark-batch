package com.mahfooz.spark.jdbc.read

import org.apache.spark.sql.SparkSession

object SparksqlJdbc {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option(
        "url",
        "jdbc:oracle:thin:@mttdevdbcl03a.intl.vsnl.co.in:1521/comtst"
      )
      .option("dbtable", "CITIES")
      .option("user", "move")
      .option("password", "access123")
      .load()

    jdbcDF.show()
  }
}
