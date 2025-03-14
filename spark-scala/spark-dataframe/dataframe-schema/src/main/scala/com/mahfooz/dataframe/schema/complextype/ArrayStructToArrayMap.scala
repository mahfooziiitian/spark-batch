package com.mahfooz.dataframe.schema.complextype

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

object ArrayStructToArrayMap {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ArrayStructToArrayMap")
      .getOrCreate()

    //struct<x: string, y: string,z: string>  to a map<string, string>
    val structureData = Seq( Row("36636","Finance",Array(Row(3000,"USA","KHE"))),
      Row("40288","Finance",Array(Row(5000,"IND","KHE"))),
      Row("42114","Sales",Array(Row(3900,"USA","KHE"))),
      Row("39192","Marketing",Array(Row(2500,"CAN","KHE"))),
      Row("34534","Sales",Array(Row(6500,"USA","KHE")) ))

    val structureSchema = new StructType().add("id",StringType)
      .add("dept",StringType)
      .add("properties",ArrayType(new StructType()
        .add("salary",IntegerType)
        .add("location",StringType)
        .add("village",StringType)
      ))

    val df = spark.createDataFrame( spark.sparkContext.parallelize(structureData),structureSchema)

    df.printSchema()
    df.show(false)

    val arrayMap = df.selectExpr("dept",
      "from_json(to_json(properties), 'array<map<string, string>>') as properties")

    arrayMap.printSchema()
    arrayMap.show(false)
  }

}
