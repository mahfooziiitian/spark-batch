The amount of memory available to each executor is controlled by spark.executor.memory.
This is divided into three sections.

1. execution memory
2. storage memory
3. reserved memory

The default division is 60% for execution memory and 40% for storage, after allowing for 300 MB for reserved memory, to safeguard against OOM errors


# execution memory

Execution memory is used for Spark shuffles, joins, sorts, and aggregations.
Since different queries may require different amounts of memory, the fraction (spark.memory.fraction is 0.6 by default) of the available memory to dedicate to this can be tricky to tune but it’s easy to adjust.


# storage memory

By contrast, storage memory is primarily used for caching user data structures and partitions derived from DataFrames.


# reserved memory.

    (execution memory + shared memory ) = (spark.memory.fraction)*(spark.executor.memory - 300 )
