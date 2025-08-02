# Shuffle Hash join

Occurs when:

1. Both sides are large
2. Broadcast is not possible
3. Join keys are equi-join compatible

Spark will:

1. Shuffle both DataFrames on join key
2. Build hash table from smaller side post-shuffle

Shuffle Hash Join is divided into 2 phases.

1. `Shuffle phase` – both datasets are shuffled
2. `Hash Join phase` – smaller side data is `hashed and bucketed` and hash joined with he bigger side in all the partitions.

Sorting is not needed with Shuffle Hash Joins inside the partitions.

## Things to Note

1. Only supported for '=' join.
2. The join keys don't need to be sortable(this will make sense below).
3. Supported for all join types except `full outer joins`.
4. In my opinion, it's an expensive join in a way that involves both shuffling and hashing(Hash Join). Maintaining a hash table requires memory and computation.

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

```SQL
SELECT *
FROM df1
JOIN df2
  ON df1.id = df2.id
```

## Performance Tips

1. Prefer broadcast hash join when one side is < 10MB
2. Use join hints: /*+ BROADCAST(df) */
3. For shuffle hash joins:
    - Ensure partitioning is balanced
    - Use `spark.sql.autoBroadcastJoinThreshold` wisely
