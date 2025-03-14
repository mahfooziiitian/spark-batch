# BHJ

Join operations are a common type of transformation in big data analytics in which two data sets, in the form of tables or DataFrames, are merged over a common matching key.
Similar to relational databases, the Spark DataFrame and Dataset APIs and Spark SQL offer a series of join transformations: 

1. inner joins
2. outer joins
3. left joins
4. right joins

All of these operations trigger a large amount of data movement across Spark executors.
At the heart of these transformations is how Spark computes what data to produce, what keys and associated data to write to the disk, and how to transfer 
those keys and data to nodes as part of operations like groupBy(), join(), agg(), sortBy(), and reduceByKey().
This movement is commonly referred to as the shuffle.
Spark has five distinct join strategies by which it exchanges, moves, sorts, groups, and merges data across executors: 

1. the broadcast hash join (BHJ)
2. shuffle hash join (SHJ)
3. shuffle sort merge join (SMJ)
4. broadcast nested loop join (BNLJ)
5. shuffle-and-replicated nested loop join (a.k.a. Cartesian product join) 

## Broadcast Hash Join

Also known as a map-side-only join, the broadcast hash join is employed when two data sets, one small (fitting in the driver’s and executor’s memory) and 
another large enough to ideally be spared from movement, need to be joined over certain conditions or columns. Using a Spark broadcast variable, the smaller data set is broadcasted by the driver to all Spark executors, as shown in Figure 7-6, and subsequently joined with the larger data set on each executor. This strategy avoids the large exchange.
BHJ: the smaller data set is broadcast to all executors
By default Spark will use a broadcast join if the smaller data set is less than 10 MB.
This configuration is set in **spark.sql.autoBroadcastJoinThreshold** you can decrease or increase the size depending on how much memory you have on each executor and in the driver.
If you are confident that you have enough memory you can use a broadcast join with DataFrames larger than 10 MB (even up to 100 MB).

A common use case is when you have a common set of keys between two DataFrames, one holding less information than the other, and you need a merged view of both. For example, consider a simple case where you have a large data set of soccer players around the world, playersDF, and a smaller data set of soccer clubs they play for, clubsDF, and you wish to join them over a common key:

### In Scala

    import org.apache.spark.sql.functions.broadcast
    val joinedDF = playersDF.join(broadcast(clubsDF), "key1 === key2")

In this code we are forcing Spark to do a broadcast join, but it will resort to this type of join by default if the 
size of the smaller data set is below the spark.sql.autoBroadcastJoinThreshold.
The BHJ is the easiest and fastest join Spark offers, since it does not involve any shuffle of the data set; all the data is available 
locally to the executor after a broadcast. You just have to be sure that you have enough memory both on the Spark driver’s and the executors’ side to hold the smaller data set in memory.

At any time after the operation, you can see in the physical plan what join operation was performed by executing:

    joinedDF.explain(mode)

In Spark 3.0, you can use joinedDF.explain('mode') to display a readable and digestible output.
The modes include 'simple', 'extended', 'codegen', 'cost', and 'formatted'.

### When to use a broadcast hash join

Use this type of join under the following conditions for maximum benefit:

When each key within the smaller and larger data sets is hashed to the same partition by Spark
When one data set is much smaller than the other (and within the default config of 10 MB, or more if you have sufficient memory)
When you only want to perform an equi-join, to combine two data sets based on matching unsorted keys
When you are not worried by excessive network bandwidth usage or OOM errors, because the smaller data set will be broadcast to all Spark executors


Specifying a value of -1 in spark.sql.autoBroadcastJoinThreshold will cause Spark to always resort to a shuffle sort merge join
