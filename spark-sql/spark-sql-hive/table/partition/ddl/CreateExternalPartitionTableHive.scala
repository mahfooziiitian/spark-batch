package com.mahfooz.spark.sqlhive.table.partition.ddl

import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateExternalPartitionTableHive {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse").replace("\\","/")
    System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home",""))

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("drop database if exists partition_db cascade")
    sql("create database if not exists partition_db")

    val filename = "zipcodes.csv"
    val dataPath = s"${sys.env.getOrElse("DATA_HOME","data")}/FileData/Csv".replace("\\","/")

    println(dataPath)
    println(sparkWarehouse)

    spark.read
      .options(Map("header"-> "true","inferSchema"->"true"))
      .csv(s"${dataPath}/${filename}")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("state")
      .csv(s"${dataPath}/${filename}_out")

    sql(s"""
        |CREATE EXTERNAL TABLE if not exists partition_db.zipcodes(
        |RecordNumber int,
        |Country string,
        |City string,
        |Zipcode int)
        |PARTITIONED BY(state string)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ','
        |LOCATION "${dataPath}/${filename}_out"
        |""".stripMargin)

    sql("select * from partition_db.zipcodes").show(false)
  }

}
