# Sort merge join

By default , Spark uses this method while joining data frames.

It’s two step process.

1. First all executors should exchange data across network to sort and re-allocate sorted partitions.
    At the end of this stage , Each executor should have same key valued data on both data frame partitions so
    that executor can do merge operation.

2. Merge is very quick thing.

 set spark.sql.autoBroadcastJoinThreshold=-1
