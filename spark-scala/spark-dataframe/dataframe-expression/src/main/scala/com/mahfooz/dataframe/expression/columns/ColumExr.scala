/*

Columns provide a subset of expression functionality.
If you use col() and want to perform transformations on that column, you must perform those on that column reference.
When using an expression, the expr function can actually parse transformations and column references from a string
and can subsequently be passed into further transformations.

expr("someCol - 5") is the same transformation as performing col("someCol") - 5, or even expr("someCol") - 5

This might be a bit confusing at first, but remember a couple of key points:

Columns are just expressions.
Columns and transformations of those columns compile to the same logical plan as parsed expressions.


 */
package com.mahfooz.dataframe.expression.columns

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr

object ColumExr {

  def main(args: Array[String]): Unit = {

    println(expr("(((someCol + 5) * 200) - 6) < otherCol"))

    println((((col("someCol") + 5) * 200) - 6) < col("otherCol"))
  }
}
