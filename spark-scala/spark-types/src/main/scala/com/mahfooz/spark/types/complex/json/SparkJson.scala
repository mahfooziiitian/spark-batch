package com.mahfooz.spark.types.complex.json

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{get_json_object,col,json_tuple}

object SparkJson {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("SparkJson")
      .master("local")
      .getOrCreate()

    val jsonDF = spark
      .range(1)
      .selectExpr("""'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

    jsonDF.select(get_json_object(col("jsonString"),
      "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey"))
      .show(2)
  }
}
