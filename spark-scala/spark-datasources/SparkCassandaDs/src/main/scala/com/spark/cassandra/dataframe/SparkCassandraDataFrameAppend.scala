package com.spark.cassandra.dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkCassandraDataFrameAppend {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val dataPath =
      s"""file:///${sys.env.getOrElse("DATA_HOME","data")
        .replace("\\","/")}/FileData/Csv/Flight/2010-summary.csv""".stripMargin
    val csvDF = spark.read
      .option("header",true)
      .option("inferSchema",true)
      .csv(dataPath)

    val csvModifiedDF=csvDF.withColumnRenamed("DEST_COUNTRY_NAME","DEST_COUNTRY_NAME".toLowerCase)
      .withColumnRenamed("ORIGIN_COUNTRY_NAME","ORIGIN_COUNTRY_NAME".toLowerCase)

    csvModifiedDF.printSchema()

    csvModifiedDF.write
      .mode(SaveMode.Append)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "flight",
        "keyspace" -> "transport"))
      .save()

    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "flight",
        "keyspace" -> "transport"))
      .load()

    df.show(false)
  }

}
