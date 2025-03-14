package com.mahfooz.spark.csv.schema

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object CsvSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME", "data")

    println(spark.sparkContext.defaultParallelism)
    println(spark.sparkContext.defaultMinPartitions)

    val myManualSchema = new StructType(
      Array(
        StructField("DEST_COUNTRY_NAME", StringType, true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        StructField("count", LongType, false)
      )
    )
    val csvFile = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .schema(myManualSchema)
      .load(s"${dataHome}/FileData/Csv/Flight/2010-summary.csv")

    csvFile.show(5)

    println(s"Number of partitions: ${csvFile.rdd.getNumPartitions}")

    //writing to file
    csvFile.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("sep", "\t")
      .save(s"${dataHome}/FileData/Csv/TsvFile.tsv")

  }

}
