/*

A schema is described using StructType, which is a collection of StructField objects.
StructType and StructField belong to the org.apache.spark.sql.types package.
DataTypes such as IntegerType, StringType also belong to the org.apache.spark.sql.types package.

 */
package com.mahfooz.sparksql.schema

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SparkSchema {

  def main(args: Array[String]): Unit = {

    val schema = new StructType()
      .add("i", IntegerType)
      .add("s", StringType)

    schema.printTreeString
    println(schema.prettyJson)
  }
}
