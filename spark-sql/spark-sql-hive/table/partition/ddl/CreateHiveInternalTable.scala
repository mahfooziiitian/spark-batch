package com.mahfooz.spark.sqlhive.table.partition.ddl

import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateHiveInternalTable {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE","spark-warehouse")
    System.setProperty("derby.system.home", sys.env.getOrElse("derby.system.home",""))

    val spark = SparkSession
      .builder()
      .appName("CreateHiveInternalTable")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("create database if not exists partition_db")

    val filename = "zipcodes.csv"
    val dataPath = s"${sys.env.getOrElse("DATA_HOME", "data")}/FileData/Csv"

    println("creating table ")
    sql("create database if not exists partition_db")

    //sql("drop table partition_db.zipcodes_internal")

    sql(
      """
        |CREATE TABLE if not exists partition_db.zipcodes_internal(
        |RecordNumber int,
        |Country string,
        |City string,
        |Zipcode int,
        |state string)
        |using csv
        |OPTIONS (delimiter "," )
        |PARTITIONED BY(state)
        |""".stripMargin)

    spark.read
      .options(Map("header"-> "false",
        "delimiter"-> ",",
        "inferSchema"->"true"))
      .csv(s"${dataPath}/${filename}")
      .toDF("RecordNumber","Country","City","Zipcode","state")
      .write
      .mode(SaveMode.Append)
      .format("csv")
      .partitionBy("state")
      .saveAsTable("partition_db.zipcodes_internal")

    sql("select * from partition_db.zipcodes_internal").show(false)
  }

}
