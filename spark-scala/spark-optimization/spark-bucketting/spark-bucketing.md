
It is similar to partitioning in Hive with an added functionality that it divides large 
datasets into more manageable parts known as buckets.

1. The concept of bucketing is based on the hashing technique. 
2. Here, modules of current column value and the number of required buckets is calculated (let say, F(x) % 3). 
3. Now, based on the resulted value, the data is stored into the corresponding bucket.

Bucketing is supported only for Spark-managed tables.

Bucketing is another data organization technique that groups data with the same bucket value.
It is similar to partitioning, but partitioning creates a directory for each partition, 
whereas bucketing distributes data across a fixed number of buckets by a hash on the bucket 
value.

The information about bucketing is stored in the metastore. It might be used with or without partitioning.

An important keynote is that partitioning should only be used with columns that have a limited number of values; 
bucketing works also well when the number of unique values is large.

Columns that are commonly used in aggregations and joins as keys are suitable candidates for bucketing.

By applying bucketing on the convenient columns in the data frames before shuffle required 
operations, we might avoid multiple probable expensive shuffles. 
Bucketing boosts performance by already sorting and shuffling data before performing 
sort-merge joins. 

It is important they have the same number of buckets on both sides of the tables in the join.

To use it, the number of the buckets and the key column are specified.

    df = df.bucketBy(32, "key").sortBy("value")

