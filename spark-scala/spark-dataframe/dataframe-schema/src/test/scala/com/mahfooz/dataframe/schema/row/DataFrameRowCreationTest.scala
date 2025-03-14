package com.mahfooz.dataframe.schema.row

import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

class DataFrameRowCreationTest extends AnyFunSuite {

  test("A Row object creation using field values"){
    // Create a Row from values.
    val row=Row("Mahfooz","Alam",34)

    // Create a Row from a Seq of values.
    val rowSeq=Row.fromSeq(Seq("Mahfooz","Alam",34))

    assert(row == rowSeq)
  }

  test("A value of a row can be accessed through both generic access by ordinal"){
    val row = Row(1, true, "a string", null)

    val firstValue = row(0)
    assert(firstValue==1)
    val fourthValue = row(3)
    assert(fourthValue==null)
  }
  test("testing primitive and null"){
    val row = Row(1, true, "a string", null)
    // using the row from the previous example.
    val firstValue = row.getInt(0)
    assert(firstValue==1)

    val isNull = row.isNullAt(3)
    assert(isNull)
  }
}
