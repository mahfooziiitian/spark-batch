# Shuffle Hash Join

A ShuffleHashJoin is the most basic way to join tables in Spark.
Shuffle Hash Join, as the name indicates works by shuffling both datasets.
So the same keys from both sides end up in the same partition or task.
Once the data is shuffled, the smallest of the two will be hashed into buckets and
a hash join is performed within the partition.

Shuffle Hash Join is different from Broadcast Hash Join because the entire dataset is not broadcasted instead both datasets are shuffled and then the smallest side data is hashed and bucketed and hash joined with the bigger side in all the partitions.

1. **Shuffle phase**

    Both datasets are shuffled

3. **Hash Join phase**

    Smaller side data is hashed and bucketed and hash joined with the bigger side in all the partitions.

## Hash join

In spark, Hash Join plays a role at per node level and the
strategy is used to join partitions available on the node.

1. create hash table based join key of small table
2. Loop over large table and matched the hashed join key.

Hash Join is performed by first creating a Hash Table based on join_key of smaller relation
and then looping over larger relation to match the hashed join_key values.

**Also, this is only supported for '=' join.**

## Broadcast Hash Join

A BroadcastHashJoin is also a very common way for Spark to join two tables under
the special condition that one of your tables is small.

1. Broadcast the data on each worker node.
2. create hash table based join key of small table
3. Loop over large table and matched the hashed join key.

no shuffling and parallelism is maintained.
In broadcast hash join, copy of one of the join relations are being sent to all the worker nodes and it saves shuffling cost.

This is useful when you are joining a large relation with a smaller one. It is also known as map-side join(associating worker nodes with mappers).

Spark deploys this join strategy when the size of one of the join relations is less than the threshold values(default 10 M).
The spark property which defines this threshold is

      spark.sql.autoBroadcastJoinThreshold(configurable).

Broadcast relations are shared among executors using the BitTorrent protocol

The broadcasted relation should fit completely into the memory of each executor as well as the driver. In Driver, because driver will start the data transfer.
Only supported for ‘=’ join.
Supported for all join types(inner, left, right) except full outer joins.
When the broadcast size is small, it is usually faster than other join strategies.
Copy of relation is broadcasted over the network. Therefore, being a network-intensive operation could cause out of memory errors or performance issues when broadcast size is big(for instance, when explicitly specified to use broadcast join/changes in the default threshold).
You can’t make changes to the broadcasted relation, after broadcast. Even if you do, they won’t be available to the worker nodes(because the copy is already shipped).

## Dealing with Key Skew in a ShuffleHashJoin

Key Skew is a common source of slowness for a Shuffle Hash Join.

## Cartesian Join

– Cartesian Joins is a hard problem.

## One to Many Joins

When a single row in one table can match to many rows in your other table,
the total number of output rows in your joined table can be really high.

## Theta Joins

If you aren’t joining two tables strictly by key, but instead checking on a
condition for your tables, you may need to provide some hints to Spark SQL to
get this to run well.
