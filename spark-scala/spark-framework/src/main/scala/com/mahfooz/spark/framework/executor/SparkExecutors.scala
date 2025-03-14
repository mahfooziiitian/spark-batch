/*
A process launched for an application on a worker node, that runs tasks and keeps data in memory or disk storage across them.
Each application has its own executors.
Spark executors are the processes that perform the tasks assigned by the Spark driver.
Executors have one core responsibility: take the tasks assigned by the driver, run them, and report back their
state (success or failure) and results.
Each Spark Application has its own separate executor processes.
Each Spark executor is a JVM process and is exclusively allocated to a specific Spark
application.

Runs the tasks, return results to the driver.

Offers in memory storage for RDDs that are cached by user programs.

Multiple executors per nodes possible



 */
package com.mahfooz.spark.framework.executor

object SparkExecutors {

}
