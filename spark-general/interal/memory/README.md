# Spark Memory Management

Spark's memory is divided into:

1. Storage Memory: Used for caching RDDs and DataFrames.
2. Execution Memory: Used for computation, like shuffling and sorting.
3. User Memory: Allocated for user-defined functions.
4. Reserved Memory: Reserved for Spark’s internal use.
