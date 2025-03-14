
# partition  = number of cpu cores in machine

Spark by default partitions data based on the number of cores available or the number of Hadoop Distributed File System (HDFS) blocks.
f we need to partition the data in any other custom format, then Spark provides options for that too.

# Data in a partition exists on single node on cluster.

# Make # partition a multiple of # cores

# number of shuffle partition

# partition size ~ 200 MB

# enable AQE
    spark.conf.get("spark.sql.adaptive.enabled")

# designing for efficiency and performance:

1. Partition datasets into smaller chunks that can be run with optimal parallelism for multiple queries.
2. Partition the data such that queries don't end up requiring too much data from other partitions
3. Minimize cross-partition data transfers.
4. Design effective folder structures to improve the efficiency of data reads and writes.
5. Partition data such that a significant amount of data can be pruned while running queries.
6. Partition in units of data that can be easily added, deleted, swapped, or archived. This helps improve the efficiency of data life cycle management.
7. File sizes in the range of 256 megabytes (MB) to 100 gigabytes (GB) perform really well with analytical engines such as HDInsight and Azure Synapse. So, aggregate the files to these ranges before running the analytical engines on them.
8. For I/O-intensive jobs, try to keep the optimal I/O buffer sizes in the range of 4 to 16 MB; anything too big or too small will become inefficient.
9. Run more containers or executors per virtual machine (VM) (such as Apache Spark executors or Apache Yet Another Resource Negotiator (YARN) containers).

