RDD Lineage (aka RDD operator graph or RDD dependency graph) is a graph of all the parent RDDs of a RDD.

It is built as a result of applying transformations to the RDD and creates a logical execution plan.

A RDD lineage graph is hence a graph of what transformations need to be executed after an action has been called.


