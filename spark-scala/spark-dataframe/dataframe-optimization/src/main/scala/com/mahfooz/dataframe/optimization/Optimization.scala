/*

Simple DataFrame transformations allow us to do most of the standard things one can do when working a row at a time.
You can still do many of the same operations defined on RDDs, except using Spark SQL expressions instead of
arbitrary functions.
DataFrame functions, like filter, accept Spark SQL expressions instead of lambdas.
These expressions allow the optimizer to understand what the condition represents, and with filter,
it can often be used to skip reading unnecessary records.

 */
package com.mahfooz.dataframe.optimization

object Optimization {
  def main(args: Array[String]): Unit = {

  }
}
