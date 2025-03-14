/*

Execution plan is as given below.

  1)  Write DataFrame/Dataset/SQL Code.
  2)  If valid code, Spark converts this to a Logical Plan.
  3)  Spark transforms this Logical Plan to a Physical Plan, checking for optimizations along the way.
  4)  Spark then executes this Physical Plan (RDD manipulations) on the cluster.

To execute code, we must write code. This code is then submitted to Spark either through the console or via a
submitted job.
This code then passes through the Catalyst Optimizer, which decides how the code should be executed and lays out a
plan for doing so before, finally, the code is run and the result is returned to the user.

 */
package com.mahfooz.spark.optimizer.plan

object ExecutionPlan {

}
