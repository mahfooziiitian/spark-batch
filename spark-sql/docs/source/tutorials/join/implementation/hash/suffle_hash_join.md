# Shuffle hash join

The shuffle hash join implementation consists of two steps.

1. The first step is to compute the `hash value` of the columns in the `join expression` of each row in each dataset and then shuffle those rows with the same hash value to the same partition. To determine which partition a particular row will be moved to, Spark performs a simple arithmetic operation, which computes the modulo of the hash value by the number of partitions.
2. Once the first step is completed, the second step combines the columns of those rows that have the same column hash value.

At a high level, these two steps are similar to the steps in the MapReduce programming model.

