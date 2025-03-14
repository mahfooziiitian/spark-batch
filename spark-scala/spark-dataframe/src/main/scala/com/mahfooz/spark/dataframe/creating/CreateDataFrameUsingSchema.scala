package com.mahfooz.spark.dataframe.creating

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CreateDataFrameUsingSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CreateDataFrameUsingSchema")
      .master("local[*]")
      .getOrCreate()


    val peopleRDD = spark.sparkContext.parallelize(Array(Row(1L, "John Doe", 30L),
      Row(2L, "Mary Jane", 25L)))

    val schema = StructType(Array(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", LongType, true)
    ))

    val peopleDF = spark.createDataFrame(peopleRDD, schema)

    peopleDF.printSchema

    peopleDF.show

  }

}
