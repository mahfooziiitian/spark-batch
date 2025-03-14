/**
 *Hash Partitioner :-
 * Splits our data in such way that elements with the same hash (can be key, keys, or a function) will be in the same partition. We can also pass wanted number of partitions, so that the final determined partition will be hash % numPartitions. Notice that if numPartitions is bigger than the number of groups with the same hash, there would be empty partitions.
Example of use: df.repartiton(10, 'class_id')

 */
package com.mahfooz.dataframe.partition.repartition.hash

object HashPartitionerEx {

}
