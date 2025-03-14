/*

In Spark, each row in a DataFrame is a single record. Spark represents this record as an object of type Row.
Spark manipulates Row objects using column expressions in order to produce usable values.
Row objects internally represent arrays of bytes.
The byte array interface is never shown to users because we only use column expressions to manipulate them.
commands that return individual rows to the driver will always return one or more Row types when we are
working with DataFrames.
We use lowercase “row” and “record” interchangeably in this chapter, with a focus on the latter.
A capitalized Row refers to the Row object.
 */
package com.mahfooz.schema.table.record

object SparkRecord {
  def main(args: Array[String]): Unit = {

  }
}
