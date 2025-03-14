package com.mahfooz.sparksql.schema.check

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SchemaCheck {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val simpleData = Seq(Row("James","","Smith","36636","M",3000),
      Row("Michael","Rose","","40288","M",4000),
      Row("Robert","","Williams","42114","M",4000),
      Row("Maria","Anne","Jones","39192","F",4000),
      Row("Jen","Mary","Brown","","F",-1)
    )

    val simpleSchema = StructType(Array(
      StructField("firstname",StringType,nullable = true),
      StructField("middlename",StringType,nullable = true),
      StructField("lastname",StringType,nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("salary", IntegerType, nullable = true)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(simpleData),simpleSchema)

    println(df.schema.fieldNames.contains("firstname"))
    println(df.schema.contains(StructField("firstname",StringType,nullable = true)))
  }
}
