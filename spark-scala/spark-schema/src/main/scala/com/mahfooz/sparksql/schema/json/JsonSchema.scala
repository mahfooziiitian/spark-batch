package com.mahfooz.sparksql.schema.json

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.io.Source

object JsonSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(JsonSchema.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val structureData = Seq(
      Row(Row("James","","Smith"),"36636","M",3100),
      Row(Row("Michael","Rose",""),"40288","M",4300),
      Row(Row("Robert","","Williams"),"42114","M",1400),
      Row(Row("Maria","Anne","Jones"),"39192","F",5500),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val url = ClassLoader.getSystemResource("schema.json")
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]

    val df= spark.createDataFrame(
      spark.sparkContext.parallelize(structureData),schemaFromJson)
    df.printSchema()
    println(df.schema.prettyJson)
  }
}
