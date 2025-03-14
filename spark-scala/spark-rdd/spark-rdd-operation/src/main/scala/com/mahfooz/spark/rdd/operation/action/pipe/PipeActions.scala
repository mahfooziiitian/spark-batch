/*

The pipe method is probably one of Spark’s more interesting methods.

With pipe, you can return an RDD created by piping elements to a forked external process.

The resulting RDD is computed by executing the given process once per partition.

All elements of each input partition are written to a process’s stdin as lines of input separated by a
newline.

The resulting partition consists of the process’s stdout output, with each line of stdout resulting in one
element of the output partition.

A process is invoked even for empty partitions.

 */
package com.mahfooz.spark.rdd.operation.action.pipe

object PipeActions {
  def main(args: Array[String]): Unit = {

    //words.pipe("wc -l").collect()

  }
}
