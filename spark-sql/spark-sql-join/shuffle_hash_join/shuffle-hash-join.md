# Shuffle Hash join

Shuffle Hash Join is divided into 2 phases.

1. `Shuffle phase` – both datasets are shuffled
2. `Hash Join phase` – smaller side data is `hashed and bucketed` and hash joined with he bigger side in all the partitions.

Sorting is not needed with Shuffle Hash Joins inside the partitions.

## Things to Note

1. Only supported for '=' join.
2. The join keys don't need to be sortable(this will make sense below).
3. Supported for all join types except `full outer joins`.
4. In my opinion, it's an expensive join in a way that involves both shuffling and hashing(Hash Join). Maintaining a hash table requires memory and computation.
