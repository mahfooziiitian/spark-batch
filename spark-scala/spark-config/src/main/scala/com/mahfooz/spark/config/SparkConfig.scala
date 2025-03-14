package com.mahfooz.spark.config

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkConfig {

  var logger=LoggerFactory.getLogger(SparkConfig.getClass)

  def main(args: Array[String]): Unit = {

    if(args.length!=1){
      logger.error("usage(): <json file name>")
      System.exit(1)
    }

    val spark=SparkSession.builder()
      .appName("spark-config")
      .config("spark.master","local")
      .getOrCreate()

    //fileName
    val fileName=args(0)

    //Reading json file
    val df=spark.read.format("json")
      .option("inferSchema","true")
      .load(fileName)

    df.show()

    spark.stop()
  }

}
