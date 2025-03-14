/*

When using partitionBy(), you have to be very cautious with the number of partitions it creates, as having too many partitions creates too many sub-directories in a directory which brings unnecessarily and overhead to NameNode (if you are using Hadoop) since it must keep all metadata for the file system in memory.
Let’s assume you have a US census table that contains zip code, city, state, and other columns. Creating a partition on the state, splits the table into around 50 partitions, when searching for a zipcode within a state (state=’CA’ and zipCode =’92704′) results in faster as it needs to scan only in a state=CA partition directory.

Partition on zipcode may not be a good option as you might end up with too many partitions.

Another good example of partition is on the Date column. Ideally, you should partition on Year/Month but not on a date.

Too Many Partitions Good?

If you are a beginner, you would think too many partitions will boost the Spark Job Performance actually, it won’t and it’s overkill.
Spark has to create one task per partition and most of the time goes into creating, scheduling, and managing the tasks then executing.

7. Too Few Partitions Good?
Too few partitions are not good as well, as you may not fully utilize your cluster resources.

Less parallelism
Applications may run longer as each partition takes more time to complete.

 */
package com.mahfooz.dataframe.partition.bps

object SparkPartitionBps {

  def main(args: Array[String]): Unit = {

  }

}
