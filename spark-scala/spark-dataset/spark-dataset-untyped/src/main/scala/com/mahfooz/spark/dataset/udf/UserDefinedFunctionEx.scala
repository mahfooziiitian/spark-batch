package com.mahfooz.spark.dataset.udf

import org.apache.spark.sql.SparkSession

object UserDefinedFunctionEx {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("UserDefinedFunctionEx")
      .master("local[*]")
      .getOrCreate()

    val dataPath =
      s"file:///${sys.env.getOrElse("DATA_HOME", "data").replace("\\", "/")}/FileData/Json/person.json"

    val personDF = spark.read.json(dataPath)

    personDF.createOrReplaceTempView("person")

    spark.udf.register("calculateAverage", CalculateAverage)
    val result =
      spark.sql("SELECT calculateAverage(age) as average_age FROM person")

    result.show()

  }

}
