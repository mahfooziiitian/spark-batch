# Spark Shuffle Mechanism

Shuffle occurs when data moves between different stages:

1. Map Phase: Data is partitioned and written to disk.
2. Reduce Phase: Tasks read shuffled data and perform aggregations.

## To optimize shuffling

1. Tungsten Optimization: Uses binary processing and code generation.
2. Sort-Based Shuffle: Avoids excessive disk I/O.
3. Broadcast Joins: Used when one dataset is small enough to be broadcasted.
