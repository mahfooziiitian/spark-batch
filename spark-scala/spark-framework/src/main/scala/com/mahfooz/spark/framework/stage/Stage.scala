/*

Each job gets divided into smaller sets of tasks called stages that depend on each other.
The operators that don’t require the data to be shuffled across the partitions are grouped together as a stage.
Examples are map, filter, and so on.
The operators that require the data to be shuffled are grouped together as a stage.
An example is reduceByKey.

 */
package com.mahfooz.spark.framework.stage

class Stage {

}
