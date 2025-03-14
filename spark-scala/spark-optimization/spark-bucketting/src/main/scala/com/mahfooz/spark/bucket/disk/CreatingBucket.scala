package com.mahfooz.spark.bucket.disk

import org.apache.spark.sql.SparkSession

object CreatingBucket {

  def main(args: Array[String]): Unit = {

    val sparkWarehouse = sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse")
    val derbySystemHome = sys.env.getOrElse("derby.system.home", "metastore_db")

    System.setProperty("derby.system.home",derbySystemHome)

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("CreatingBucket")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    val numberBuckets = 10
    val columnToBucketBy = "count"

    val dataHome = sys.env.getOrElse("DATA_HOME","data")+"/FileData/Csv/Flight"
    val csvFile = spark.read.format("csv")
      .option("inferSchema",true)
      .option("header","true")
      .load(s"${dataHome}/2010-summary.csv")

    spark.sql("drop table if exists flight_bucket")

    csvFile.write.format("parquet")
      .mode("overwrite")
      .bucketBy(numberBuckets, columnToBucketBy)
      .saveAsTable("flight_bucket")
  }

}
