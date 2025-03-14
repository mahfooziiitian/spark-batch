package com.mahfooz.dataframe.partition.by

import org.apache.spark.sql.{SaveMode, SparkSession}

object PartitionBy {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(PartitionBy.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val dataHome = sys.env.getOrElse("DATA_HOME","data")+"/FileData/Json/Movies"
    val movies = spark.read
      .format("json")
      .load(s"${dataHome}/movies.json")

    movies.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .partitionBy("produced_year")
      .save(s"${dataHome}/movies_partition")
  }
}
