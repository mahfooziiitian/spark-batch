"""
Internally, Spark uses five main properties to represent an RDD.

The three required properties:

1. List of partition objects that make up the RDD
2. A function for computing an iterator of each partition
3. A list of dependencies on other RDDs
4. Optionally, RDDs also include a partitioner (for RDDs of rows of key/value pairs represented as Scala tuples)
5. A list of preferred locations (for the HDFS file).

As an end user, you will rarely need these five properties and are more likely to use
predefined RDD transformations.

However, it is helpful to understand the properties and know how to access them for debugging and for a better
conceptual understanding.
"""