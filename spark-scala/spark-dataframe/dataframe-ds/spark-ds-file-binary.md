# Parquet

It is one of the most popular open source columnar storage formats in the Hadoop
ecosystem, and it was created at Twitter. Its popularity is because it is a self-describing data format and it stores data in a highly compact structure by leveraging compressions.
The columnar storage format is designed to work well with a data analytics workload
where only a small subset of the columns are used during the data analysis.
Parquet stores the data of each column in a separate file; therefore, columns that are not needed in a data analysis wouldn’t have to be unnecessarily read in.

It is quite flexible when it comes to supporting a complex data type with a nested structure.
Text file formats such as CVS and JSON are good for small files, and they are human-readable.

For working with large datasets that are stored in long-term storage, Parquet is a much better file format to use:

1. reduce storage costs
2. to speed up the reading

Spark works extremely well with the Parquet file format, and in fact Parquet is the
default file format for reading and writing data in Spark.
Since Parquet files are self-contained, meaning the schema is stored inside the Parquet data file, it is easy to work with Parquet in Spark.

Notice that you don’t need to provide a schema or ask Spark to infer the schema. Spark can retrieve the schema from the Parquet file.

**One of the cool optimizations that Spark does when reading data from Parquet is that it does decompression and decoding in column batches.**
