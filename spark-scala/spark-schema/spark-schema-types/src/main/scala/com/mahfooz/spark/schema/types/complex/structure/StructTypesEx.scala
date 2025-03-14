/*

Represents values with the structure described by a sequence of StructFields (fields).

  StructField(name, dataType, nullable):

Represents a field in a StructType.
name indicates the name of a field, and datatype indicates the data type of a field.
nullable is used to indicate if values of this fields can have null values.

*/
package com.mahfooz.spark.schema.types.complex.structure

import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object StructTypesEx {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName(StructTypesEx.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Array(Row(ArrayBuffer(1,2,3,4))))

    val df = spark.sqlContext.createDataFrame(
      rdd,
      StructType(Seq(StructField("list", ArrayType(IntegerType, false), false))))

    df.printSchema

    df.show()
  }

}
