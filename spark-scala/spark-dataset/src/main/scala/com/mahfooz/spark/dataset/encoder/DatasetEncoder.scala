/*

A Dataset has helpers called encoders, which are smart and efficient encoding utilities that convert data inside each user-defined object into a
compact binary format.
This translates into a reduction of memory usage if and when a Dataset is cached in memory as well as a reduction in the number of bytes that
Spark needs to transfer over a network during the shuffling process.

 */
package com.mahfooz.spark.dataset.encoder

object DatasetEncoder {
  def main(args: Array[String]): Unit = {}
}
