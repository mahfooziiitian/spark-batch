/*
Spark uses the catalog, a repository of all table and DataFrame information, to resolve columns and tables in the
analyzer.

 The analyzer might reject the unresolved logical plan if the required table or column name does
 not exist in the catalog.

 If the analyzer can resolve it, the result is passed through the Catalyst Optimizer, a collection of rules that
 attempt to optimize the logical plan by
 pushing down predicates or selections.
 Packages can extend the Catalyst to include their own rules for domain-specific optimizations.

 */
package com.mahfooz.spark.framework.plan.logical.resolved

object ResolvedLogicalPlan {

}
