package com.mahfooz.config

import org.apache.spark.sql.SparkSession

object EnvConfig {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(EnvConfig.getClass.getName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir",sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse"))
      .getOrCreate()

    spark.conf.getAll.foreach(map => println(map._1+"="+map._2))
  }

}
