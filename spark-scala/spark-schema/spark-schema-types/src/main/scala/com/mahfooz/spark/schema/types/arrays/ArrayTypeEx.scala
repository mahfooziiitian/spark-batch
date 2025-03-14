package com.mahfooz.spark.schema.types.arrays

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object ArrayTypeEx {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("ArrayTypeEx")
      .master("local[*]")
      .getOrCreate()

    val schema= StructType(List(
      StructField("team_name", StringType, nullable = true),
      StructField("top_players", ArrayType(StringType, containsNull = true), nullable = true)))

    schema.printTreeString()
    
  }

}
