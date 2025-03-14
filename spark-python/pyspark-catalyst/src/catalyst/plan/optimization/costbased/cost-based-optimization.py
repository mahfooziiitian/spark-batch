"""
The cost-based optimizations were introduced in Spark 2.2 to enable Catalyst to be more intelligent in selecting the
right kind of join based on the statistics of the data being processed.

The cost-based optimization relies on the detailed statistics of the columns participating in the filter or join
conditions, and that's why the statistics collection framework was introduced.

Examples of the statistics include the cardinality, the number of distinct values, max/min, average/max length etc.

"""

