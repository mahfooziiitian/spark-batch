/*

An executor is a JVM (Java virtual machine) process that Spark creates on each worker for an application.

It executes application code concurrently in multiple threads.

It can also cache data in memory or disk.

An executor has the same lifespan as the application for which it is created.

When a Spark application terminates, all the executors created for it also terminate.

Runs the tasks, return results to the driver.

Offers in memory storage for RDDs that are cached by user programs.

Multiple executors per nodes possible


 */
package spark.model.executor;

public class ExecutorModel {
}
