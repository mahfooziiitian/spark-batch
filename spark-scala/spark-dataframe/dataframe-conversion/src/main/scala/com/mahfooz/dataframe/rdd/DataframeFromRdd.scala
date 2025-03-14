package com.mahfooz.dataframe.rdd

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}

object DataframeFromRdd {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DataframeFromRdd")
      .getOrCreate()

    val schema = StructType(
      Array(
        StructField("id", LongType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("age", LongType, nullable = true)
      )
    )

    val peopleRDD = spark.sparkContext.parallelize(
      Array(Row(1L, "John Doe", 30L), Row(2L, "Mary Jane", 25L))
    )

    val peopleDF = spark.createDataFrame(peopleRDD, schema)

    peopleDF.printSchema

    peopleDF.show()
  }

}
