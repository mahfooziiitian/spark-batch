/*

An external service for acquiring resources on the cluster (e.g. standalone manager, Mesos, YARN)

The Spark Driver and Executors do not exist in a void, and this is where the cluster manager comes in.
The cluster manager is responsible for maintaining a cluster of machines that will run your Spark Application(s).

$SPARK_HOME/sbin:
This folder contains all the scripts which help administrators in starting and stopping the Spark cluster.

A cluster manager will have its own "driver" (sometimes called master) and "worker" abstractions.
The circles represent daemon processes running on and managing each of the individual worker nodes.
There is no Spark Application running as of yet—these are just the processes from the cluster manager.

 */
package com.mahfooz.spark.manager

class ClusterManager {

}
