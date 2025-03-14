/*
Spark maintains each RDD’s lineage (i.e., previous RDD on which it depends) that is created in DAG to achieve
fault tolerance.
When any node crashes, Spark Cluster Manager assigns another node to continue processing.
Spark does this by reconstructing the series of operations that it should compute on that partition from the source

 */
package com.mahfooz.spark.dag.tolerance

class FaultTolerance {

}
