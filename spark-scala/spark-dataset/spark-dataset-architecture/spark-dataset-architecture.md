Despite the similarity with RDD code, this code is building a query plan, not dealing with individual objects.

Additionally, if age is the only attribute accessed, then the rest of the object’s data will not be read from off-heap storage.

# How dataset is stored in memory
Dataset still keeps everything in off-heap memory but translates to JVM objects when needed.

# catalyst optimizer
Catalyst Optimizer is an engine which builds a tree of operations based on the received queries.

Catalyst contains a general library for representing trees and applying rules to manipulate them. Trees can be manipulated using rules which are functions from a tree to another tree.


