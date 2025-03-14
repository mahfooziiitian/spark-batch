package com.mahfooz.spark.dataframe.row

import org.apache.spark.sql.Row

object CreateRow {
  def main(args: Array[String]): Unit = {
    val myRow = Row("Hello", null, 1, false)
    println(myRow(0)) // type Any
    println(myRow(0).asInstanceOf[String]) // String
    println(myRow.getString(0)) // String
    println(myRow.getInt(2)) // Int
  }
}
