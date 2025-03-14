/*
You can create rows by manually instantiating a Row object with the values that belong in each column
It’s important to note that only DataFrames have schemas. Rows themselves do not have schemas.
This means that if you create a Row manually, you must specify the values in the same order as the schema of the DataFrame to which they might be appended.
 */
package com.mahfooz.schema.table.row

import org.apache.spark.sql.Row

object CreatingRow {

  def main(args: Array[String]): Unit = {
    val myRow = Row("Hello", null, 1, false)
    println(myRow)
  }

}
