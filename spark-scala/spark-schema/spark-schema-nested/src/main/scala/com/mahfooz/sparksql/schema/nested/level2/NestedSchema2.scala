package com.mahfooz.sparksql.schema.nested.level2

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object NestedSchema2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val arrayArrayData = Seq(
      Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
      Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
      Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
    )

    val arrayArraySchema = new StructType().add("name",StringType)
      .add("subjects",ArrayType(ArrayType(StringType)))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
    df.printSchema()
    df.show()
  }
}
