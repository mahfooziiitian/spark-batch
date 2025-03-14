/*

Spark employs a master-slave architecture, where the Spark driver is the master and the Spark executor is the slave.
Each of these components runs as an independent process on a Spark cluster.

Here’s an overview of the steps:

Write DataFrame/Dataset/SQL Code.

If valid code, Spark converts this to a Logical Plan.

Spark transforms this Logical Plan to a Physical Plan, checking for optimizations along the way.

Spark then executes this Physical Plan (RDD manipulations) on the cluster.


 */
package com.mahfooz.spark.framework

class SparkFrameworks {

}
