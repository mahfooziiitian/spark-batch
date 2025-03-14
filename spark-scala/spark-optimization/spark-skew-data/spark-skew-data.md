# Skew data

we have data month wise and november and december data is very huge compared to other month data.

Such an uneven distribution of data is referred to as a data skew.

Now, if we were to distribute the monthly data to individual compute nodes, the nodes that are processing the data for November and December are going to take a lot more time than the ones processing the other months.

And if we were generating an annual report, then all the other stages would have to wait for the November and December stages to complete.

Such wait times are inefficient for job performance. 

To make the processing more efficient, we will have to find a way to assign similar amounts of processing data to each of to compute nodes.


# Fixing skews at the storage level

1. Find a better distribution/partition strategy that would balance the compute time evenly. In our example of the monthly trip count, we could explore partitioning the data into smaller chunks at the weekly level or try to partition along a different dimension, such as ZIP codes altogether, and see whether that helps.

2. Add a second distribution key. If the primary key is not splitting the data evenly, and if the option of moving to a new distribution key is not possible, you could add a secondary partition key. For example, after the data is split into months, you can further split them into, say, states of the country within each month. That way, if you are in the USA, you get 50 more splits, which could be more evenly distributed.

3. Randomize the data and use the round-robin technique to distribute the data evenly into partitions. If you are not able to find an optimal distribution key, then you can resort to round-robin distribution. This will ensure that the data is evenly distributed.

**Note that it might not always be possible to recreate tables or distributions.**

So, some pre-planning is very important when we first decide on the partition strategy.

However, if we end up in a situation where the partitioning strategies are not helping, we might still have one more option left to improve our skew handling.'

This option is to trust our compute engine to produce an intelligent query plan that is aware of the data skew.

# Fixing skews at to compute level

Few techniques for fixing skews at compute level:

**1. Improving the query plan by enabling statistics.**
    
Once we enable statistics, query engines such as the Synapse SQL engine, which uses a cost-based optimizer, will utilize statistics to generate the most optimal plan based on the cost associated with each plan. The optimizer can identify data skews and automatically apply the appropriate optimizations in place to handle skew.

**2. Ignore the outlier data if not significant**

This is probably the simplest of the options, but might not be applicable to all situations. If the data that is skewed is not very important, then you can safely ignore it.

While we are on the topic of handling skews at compute level, Synapse Spark has a very helpful feature that helps identify data skews in each stage of the Spark job.


# Salting
Salting a data set basically means adding randomization to the data to help it to be 
distributed more uniformly. 

An extra processing cost is paid in return for evenly distributed data across the partitions, 
and so performance gains. 

In aggregations and joins, all records with the same key are located in the same partition. 

For this reason, if one of the keys has more records compared to the others, the partition of 
that key has much more records to be processed. 

Salting technique is applied only to the skewed key, and in this sense, random values are 
added to the key. 

Then, <key1+random_salting_value> is obtained, and this created new key values are matched 
with the replicated corresponding key values in the other table if it is a join operation.

To clarify it, take a look at the following example where the key column is city information in join, and the distribution of the key column is highly skewed in tables.

To distribute the data evenly, we append random values from 1 to 5 to the end of key values for the bigger table of join and compose a new column in the smaller table by exploding an array from 1 to 5.




