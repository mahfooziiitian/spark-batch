package com.mahfooz.dataframe.action

import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

object CollectAsListAction {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CollectAsListAction")
      .getOrCreate()

    val arrayStructData = Seq(
      Row("James", "Java"),
      Row("James", "C#"),
      Row("James", "Python"),
      Row("Michael", "Java"),
      Row("Michael", "PHP"),
      Row("Michael", "PHP"),
      Row("Robert", "Java"),
      Row("Robert", "Java"),
      Row("Robert", "Java"),
      Row("Washington", null)
    )

    val arrayStructSchema = new StructType()
      .add("name", StringType)
      .add("booksInterested", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)

    df.printSchema()
    df.show(false)

    val df2 = df.groupBy("name")
      .agg(collect_list("booksInterested")
      .as("booksInterested"))

    df2.printSchema()
    df2.show(false)

    df.groupBy("name")
      .agg(collect_set("booksInterested")
      .as("booksInterested"))
      .show(false)
  }

}
