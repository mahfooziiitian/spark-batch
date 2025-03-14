/*
Spark types map directly to the different language APIs that Spark maintains and there exists a lookup table
for each of these in Scala, Java, Python, SQL, and R.
Even if we use Spark’s Structured APIs from Python or R, the majority of our manipulations will operate
strictly on Spark types, not Python types.
For example, the following code does not perform addition in Scala or Python; it actually performs addition
purely in Spark.

This addition operation happens because Spark will convert an expression written in an input language to
Spark’s internal Catalyst representation of that same type information.
It then will operate on that internal representation.

A schema is a StructType made up of a number of fields, StructFields, that have a name, type, a Boolean flag which specifies whether that column can contain missing or null values, and, finally, users can optionally specify associated metadata with that column. The metadata is a way of storing information about this column (Spark uses this in its machine learning library).

 */
package com.mahfooz.spark.schema.types

import org.apache.spark.sql.SparkSession

object SparkTypes {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkTypes")
      .getOrCreate()

    val df = spark.range(500).toDF("number")
    df.select(df.col("number") + 10).show()
  }
}
