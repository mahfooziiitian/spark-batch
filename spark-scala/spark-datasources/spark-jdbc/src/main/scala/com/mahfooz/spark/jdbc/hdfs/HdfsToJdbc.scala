package com.mahfooz.spark.jdbc.hdfs

import java.util.Properties
import org.apache.spark.sql.SparkSession

object HdfsToJdbc {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val props = new Properties()
    props.load(ClassLoader.getSystemResourceAsStream("jdbc.properties"))

    val jdbcDF2 = spark.read.csv(args(0))

    //Writing to db
    jdbcDF2.write
      .jdbc(props.getProperty("jdbcUrl"), args(1), props)
  }
}
