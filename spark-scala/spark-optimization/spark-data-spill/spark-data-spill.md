# Data spill

Data spill refers to the process where a compote engine such as SQL or Spark, while executing a query, is unable to hold the required data in memory and writes (spills) to disk.

This results in increased query execution time due to the expensive disk reads and writes.

Spills can occur for any of the following reasons:

1. The data partition size is too big.
2. To compute resource size is small, especially the memory.
3. The exploded data size during merges, unions, and so on exceeds the memory limits of the compute node.

# Solutions for handling data spills would be as follows:

**1. Increase to compute capacity**

Especially the memory if possible.
This will incur higher costs, but is the easiest of the options.

**2. Reduce the data partition sizes**

Repartition if necessary. 
This is more effort-intensive as repartitioning takes time and effort.
If you are not able to afford the higher compute resources, then reducing the data partition sizes is the best option.

**3. Remove skews in data** 

Sometimes, it is the data profile that causes the spills.

If the data is skewed, it might cause spills in the partitions with the higher data size.

# Identifying data spills in Spark
Identifying spills in Spark is relatively straightforward as Spark publishes metrics for spills. 

