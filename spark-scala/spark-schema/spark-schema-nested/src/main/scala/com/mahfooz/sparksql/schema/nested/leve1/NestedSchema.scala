package com.mahfooz.sparksql.schema.nested.leve1

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object NestedSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val structureData = Seq(
      Row(Row("James", "", "Smith"), "36636", "M", 3100),
      Row(Row("Michael", "Rose", ""), "40288", "M", 4300),
      Row(Row("Robert", "", "Williams"), "42114", "M", 1400),
      Row(Row("Maria", "Anne", "Jones"), "39192", "F", 5500),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
    )

    val structureSchema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureSchema)
    df.printSchema()
    df.show()
  }
}
