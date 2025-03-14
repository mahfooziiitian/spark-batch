package com.mahfooz.spark.hive.ddl

import com.mahfooz.spark.hive.common.SparkHiveCommon

object CreateHiveTable {


  def main(args: Array[String]): Unit = {
    //1. Creating spark session
    val spark = SparkHiveCommon.createSparkSession("CreateHiveTable")

    //2. Create table using hive
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")

    //3. Loading data from local system into table
    sql("LOAD DATA LOCAL INPATH 'd:/data/spark/hive/kv1.txt' INTO TABLE src")

    //4. Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()

    spark.stop()
  }
}
