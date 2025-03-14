/*

An example of this is something called predicate pushdown on DataFrames.
If we build a large Spark job but specify a filter at the end that only requires us to fetch one row from our
source data, the most efficient way to execute this is to access the single record that we need.
Spark will actually optimize this for us by pushing the filter down automatically.

 */
package com.mahfooz.spark.optimizer.pushdown

object Pushdown {

}
