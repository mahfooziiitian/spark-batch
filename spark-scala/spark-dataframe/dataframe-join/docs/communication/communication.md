# Communication strategy

## Node to node communication

Spark approaches cluster communication in two different ways during joins.
It either incurs a shuffle join, which results in an all-to-all communication or a broadcast join.
Some of these internal optimizations are likely to change over time with new improvements to the cost-based optimizer
and improved communication strategies.
When you join a big table to another big table, you end up with a shuffle join.

## Broadcast communication strategy

This join strategy is applicable only when one of the datasets is small enough to fit into memory.
By default, Spark will use a broadcast join if the smaller data set is less than 10 MB.
This configuration is set in

    spark.sql.autoBroadcastJoinThreshold

You can decrease or increase the size depending on how much memory you have on each executor and in the driver.
When the table is small enough to fit into the memory of a single worker node, with some breathing room, of course, we
can optimize our join.
Although we can use a big table–to–big table communication strategy, it can often be more efficient to use a broadcast
join.
What this means is that we will replicate our small DataFrame onto every worker node in the cluster (be it located on
one machine or many).
Now this sounds expensive. However, what this does is prevent us from performing the all-to-all communication during the
entire join process.
Instead, we perform it only once at the beginning and then let each individual worker node perform the work without
having to wait or communicate with any other worker node.
At the beginning of this join will be a large communication, just like in the previous type of join.
However, immediately after that first, there will be no further communication between nodes.
This means that joins will be performed on every single node individually, making CPU the
biggest bottleneck.
For our current set of data, we can see that Spark has automatically set this up as a broadcast join by looking at the
explained plan.

## Shuffle strategy

When you join a big table to another big table, you end up with a shuffle join.

In a shuffle join, every node talks to every other node, and they share data according to which
node has a certain key or set of keys (on which you are joining).

These joins are expensive because the network can become congested with traffic, especially if your data is not partitioned
well.
This join describes taking a big table of data and joining it to another big table of data.
All worker nodes (and potentially every partition) will need to communicate with one another during the entire join process (with no intelligent partitioning of data).


## Shuffle Hash join

Joining is one of the most expensive operations in Spark.
At a high level, there are two different strategies Spark uses to join two datasets.
They are shuffle hash join and broadcast join.
The main criteria for selecting a particular strategy is based on the size of the two datasets. When the size of both
datasets is large, then the shuffle hash join
strategy is used.
When the size of one of the datasets is small enough to fit into the memory of the executors, then the broadcast join
strategy is used.
The following sections give the details of how each joining strategy works.
Conceptually, joining is about combining the columns of the rows of two datasets that meet the condition specified in
the join expression.
To do that, those rows with the same column values need to be co-located on the same partition.

### The shuffle hash join implementation consists of two steps

1. The first step is to compute the hash value of the columns in the join expression of each row in each dataset and
   then shuffle those
   rows with the same hash value to the same partition.
   To determine which partition a particular row will be moved to, Spark performs a simple arithmetic operation, which
   computes the modulo of the hash value by the number of partitions.

2. Combines the columns of those rows that have the same column hash value.

At a high level, these two steps are similar to the steps in the MapReduce programming model.

As mentioned, this is an expensive operation because it requires moving a lot of data from across many machines over a
network.
When moving data across a network, the data will usually go through a data serialization and deserialization process.

## Sort merge

The first step is to sort the datasets and the second operation is to merge the sorted data in the partition
by iterating over the elements and according to the join key, join the rows having the same value.
