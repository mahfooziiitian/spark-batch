/*
The Spark Driver and Executors do not exist in a void, and this is where the cluster manager comes in.
The cluster manager is responsible for maintaining a cluster of machines that will run your Spark Application(s).
cluster manager will have its own "driver" (sometimes called master) and "worker" abstractions.
The cluster manager knows where the workers are located, how much memory they have, and the number of CPU cores each one has.
One of the main responsibilities of the cluster manager is to orchestrate the work by assigning it to each worker.
 */
package com.mahfooz.spark.framework.cm

object ClusterManager {

}

