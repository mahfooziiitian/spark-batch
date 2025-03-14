# Shuffle Hash Join

A ShuffleHashJoin is the most basic way to join tables in Spark.
Shuffle Hash Join, as the name indicates works by shuffling both datasets.
So the same keys from both sides end up in the same partition or task.
Once the data is shuffled, the smallest of the two will be hashed into buckets and
a hash join is performed within the partition.

Shuffle Hash Join is different from Broadcast Hash Join because the entire dataset is not broadcasted instead both datasets are shuffled and then the smallest side data is hashed and bucketed and hash joined with the bigger side in all the partitions.

## 1. **Shuffle phase**

   Both datasets are shuffled.
   Same key data are shuffled to the same executor node from both datasets.

## 2. Hash Join phase

    Smaller side data is hashed and bucketed and hash joined with the bigger side in all the partitions.

Shuffle Hash Join involves moving data with the same value of join key in the same executor node followed by Hash Join(explained above). Using the join condition as output key, data is shuffled amongst executor nodes and in the last step, data is combined using Hash Join, as we know data of the same key will be present in the same executor.

## Notes

1. Only supported for '=' join.
2. The join keys don’t need to be sortable.
3. Supported for all join types except full outer joins.
4. It seems to an expensive join in a way that involves both shuffling and hashing(Hash Join as explained above).
5. Maintaining a hash table requires memory and computation.
