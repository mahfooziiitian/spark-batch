The principal disadvantage of RDDs is that they don’t perform exceptionally well.

Specifically, whenever Spark needs to distribute the data within the cluster or 
write the data to disk, it uses Java serialization by default (although it is 
possible to apply Kryo as a faster alternative in most cases). 

The overhead of serializing individual Java and Scala objects is expensive and 
requires sending both data and structure between nodes (each serialized object 
contains the class structure and values).

There is also the overhead of garbage collection that results from creating and 
destroying individual objects.

