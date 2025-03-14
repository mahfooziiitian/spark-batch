/*
A schema is a StructType made up of a number of fields, StructFields, that have a name, type, a Boolean
flag which specifies whether that column can contain missing or null values, and, finally, users can
optionally specify associated metadata with that column.
The metadata is a way of storing information about this column (Spark uses this in its machine learning library).

 */
package com.mahfooz.spark.schema.types.metadata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, Metadata, StringType, StructField, StructType}

object SparkMetadata {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkMetadata")
      .getOrCreate()

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false,
        Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df = spark.read.format("json")
      .schema(myManualSchema)
      .load("D:\\data\\flight-data\\json\\2015-summary.json")

    df.show()
  }
}
