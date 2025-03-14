/*

The first phase of execution is meant to take user code and convert it into a logical plan.
This logical plan only represents a set of abstract transformations that do not refer to executors or drivers, it’s
purely to convert the user’s set of expressions into the most optimized version.
It does this by converting user code into an unresolved logical plan.
This plan is unresolved because although your code might be valid, the tables or columns that it refers to might
or might not exist.
Spark uses the catalog, a repository of all table and DataFrame information, to resolve columns and tables in the
analyzer.
The analyzer might reject the unresolved logical plan if the required table or column name does not exist in the
catalog.
If the analyzer can resolve it, the result is passed through the Catalyst Optimizer, a collection of rules that attempt to
optimize the logical plan by pushing down predicates or selections.
Packages can extend the Catalyst to include their own rules for domain-specific optimizations.
The logical plan is an internal representation of the user data processing logic in the form of a tree of operators and expression.


 */
package com.mahfooz.spark.optimizer.plan.logical

object LogicalPlan {

}
