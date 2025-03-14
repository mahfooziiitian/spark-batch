/*
To create a new Row, use RowFactory.create() in Java or Row.apply() in Scala.
A Row object can be constructed by providing field values.
Example:
   import org.apache.spark.sql._

   // Create a Row from values.
   Row(value1, value2, value3, ...)
   // Create a Row from a Seq of values.
   Row.fromSeq(Seq(value1, value2, ...))

A value of a row can be accessed through both generic access by ordinal, which will incur boxing overhead
for primitives, as well as native primitive access.
An example of generic access by ordinal:

 import org.apache.spark.sql._

 val row = Row(1, true, "a string", null)
  row: Row = [1,true,a string,null]
 val firstValue = row(0)
  firstValue: Any = 1
 val fourthValue = row(3)
  fourthValue: Any = null

For native primitive access, it is invalid to use the native primitive interface to retrieve a value that is null,
instead a user must check isNullAt before attempting to retrieve a value that might be null.
An example of native primitive access:
  using the row from the previous example.

 val firstValue = row.getInt(0)
  firstValue: Int = 1

 val isNull = row.isNullAt(3)
  isNull: Boolean = true

In Scala, fields in a Row object can be extracted in a pattern match. Example:
 import org.apache.spark.sql._

 val pairs = sql("SELECT key, value FROM src").rdd.map {
   case Row(key: Int, value: String) =>
     key -> value
 }

 */
package com.mahfooz.dataframe.schema.row

import org.apache.spark.sql.Row

object DataFrameRowCreation {

  def createRowFromDataFactory(): Unit = {}

  def accessingDataFrame(): Unit = {
    val row = Row(1, true, "a string", null)
    // row: Row = [1,true,a string,null]
    val firstValue = row(0)

    assert(firstValue == 1)

    val fourthValue = row(3)
    assert(fourthValue == null)
  }
  def createRowFromFields(): Unit = {
    // Create a Row from values.
    val row = Row("Mahfooz", "Alam", 23)

    // Create a Row from a Seq of values.
    val rowSeq = Row.fromSeq(Seq("Mahfooz", "Alam", 23))

    assert(row == rowSeq)
  }

  def main(args: Array[String]): Unit = {
    createRowFromFields()
    accessingDataFrame()
  }

}
