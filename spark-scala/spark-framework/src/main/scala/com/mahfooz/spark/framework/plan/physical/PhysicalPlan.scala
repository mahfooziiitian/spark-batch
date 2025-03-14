/*

After successfully creating an optimized logical plan, Spark then begins the physical planning process.
The physical plan, often called a Spark plan, specifies how the logical plan will execute on the cluster by
generating different physical execution strategies and comparing them through a cost model.

An example of the cost comparison might be choosing how to perform a given join by looking at the physical attributes of a given table (how big the table is or how big its partitions are).
Physical planning results in a series of RDDs and transformations.
This result is why you might have heard Spark referred to as a compiler—it takes queries in DataFrames, Datasets, and SQL and compiles them into RDD transformations for you.

Upon selecting a physical plan, Spark runs all of this code over RDDs, the lower-level programming interface of Spark
Spark performs further optimizations at runtime, generating native Java bytecode that can remove entire tasks or stages during execution. Finally the result is returned to the user.
 */
package com.mahfooz.spark.framework.plan.physical

object PhysicalPlan {

}
