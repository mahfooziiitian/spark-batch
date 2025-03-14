package com.mahfooz.schema.table.row

import org.apache.spark.sql.Row

object AccessingRow {

  def main(args: Array[String]): Unit = {
    val myRow = Row("Hello", null, 1, false)
    println(myRow(0))
    println(myRow(0).asInstanceOf[String])
    println(myRow.getString(0))
    println(myRow.getInt(2))
  }
}
