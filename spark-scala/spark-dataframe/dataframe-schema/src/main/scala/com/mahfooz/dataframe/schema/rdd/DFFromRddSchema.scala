package com.mahfooz.dataframe.schema.rdd

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}

object DFFromRddSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Schemas")
      .master("local")
      .getOrCreate()

    val rddSchema = new StructType(
      Array(
        new StructField("name", StringType, true),
        new StructField("class", StringType, true),
        new StructField("age", LongType, false)
      )
    )
    val myRows = Seq(Row("Mahfooz", "B.Tech", 1L), Row("Alam", "B.Tech", 2L))
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, rddSchema)
    myDf.show()
    myDf.printSchema()

  }
}
