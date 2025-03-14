/**
 * Repartition by columns takes in a target sPartition count, as well as a sequence of columns to repartition on — e.g., df.repartition(100, $”date”). This is useful for forcing Spark to distribute records with the same key to the same partition. In general, this is useful for a number of Spark operations, such as joins, but in theory, it could allow us to solve our problem as well.
 * Repartitioning by columns uses a HashPartitioner, which will assign records with the same value for the hash
 * of their key to the same partition.
 *
 * However, this approach only works if each partition key can safely be written to one file. This is because no matter how many
 * values have a certain hash value, they’ll end up in the same partition.
 *
 * Repartitioning by columns only works when you are writing to one or more small hPartitions.
 *
 */
package com.mahfooz.spark.repartition.column

object RepartitionByColumn {

  def main(args: Array[String]): Unit = {

  }

}
