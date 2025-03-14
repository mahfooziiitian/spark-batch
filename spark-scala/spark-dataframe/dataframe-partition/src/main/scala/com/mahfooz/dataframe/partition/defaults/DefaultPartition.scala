/*

By default, Spark/PySpark creates partitions that are equal to the number of CPU cores in the machine.
Data of each partition resides in a single machine.
Spark/PySpark creates a task for each partition.
Spark Shuffle operations move the data from one partition to other partitions.
Partitioning is an expensive operation as it creates a data shuffle (Data could move between the nodes)
By default, DataFrame shuffle operations create 200 partitions.
Spark/PySpark supports partitioning in memory (RDD/DataFrame) and partitioning on the disk (File system).

Local mode

When you running on local in standalone mode, Spark partitions data into the number of CPU cores you have on your
system or the value you specify at the time of creating SparkSession object.



 */
package com.mahfooz.dataframe.partition.defaults

object DefaultPartition {
  def main(args: Array[String]): Unit = {

  }
}
