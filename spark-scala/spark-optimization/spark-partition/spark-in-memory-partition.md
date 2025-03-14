# In-memory partitioning

Spark provides the following three methods to perform in-memory partitioning:

**1. repartition**

    To increase the number of partitions.

**2. coalesce**

    To decrease the number of partitions. 

**3. repartitionByRange**

    This is a specialization of the repartition command where you can specify the ranges.
