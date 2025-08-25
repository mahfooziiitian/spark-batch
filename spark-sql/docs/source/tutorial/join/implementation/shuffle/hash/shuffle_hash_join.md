# Shuffle hash join

The shuffle hash join implementation consists of two steps.

The first step is to compute the hash value of the columns in the join expression of each row in each dataset and then shuffle those rows
with the same hash value to the same partition.
To determine which partition a particular row will be moved to, Spark performs a simple arithmetic operation, which computes the modulo of
the hash value by the number of partitions.

Once the first step is completed, the second step combines the columns of those rows that have the same column hash value.
At a high level, these two steps are similar to the steps in the MapReduce programming model.

Shuffle Hash Join, as the name indicates works by shuffling both datasets. So the same keys from both sides end up in the same partition or task. Once the data is shuffled, the smallest of the two will be hashed into buckets and a hash join is performed within the partition.

Shuffle Hash Join is different from Broadcast Hash Join because the entire dataset is not broadcasted instead both datasets are shuffled and then the smallest side data is hashed and bucketed and hash joined with the bigger side in all the partitions.

Shuffle Hash Join is divided into 2 phases.

1. Shuffle phase
    Both datasets are shuffled

2. Hash Join phase
    The smaller side data is hashed and bucketed and hash joined with he bigger side in all
    the partitions.

Sorting is not needed with Shuffle Hash Joins inside the partitions.

1. spark.sql.join.preferSortMergeJoin should be set to false

    set spark.sql.join.preferSortMergeJoin=false;

2. spark.sql.autoBroadcastJoinThreshold should be set to lower value
    so Spark can choose to use Shuffle Hash Join over Sort Merge Join.

    set spark.sql.autoBroadcastJoinThreshold=2;
